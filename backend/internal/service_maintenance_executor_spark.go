package internal

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/spf13/cast"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

type OptimizeResult struct {
	Database         string `json:"database"`
	Table            string `json:"table"`
	TargetFileSizeMb int    `json:"target_file_size_mb"`
	Where            string `json:"where"`
	ApplicationName  string `json:"application_name"`
	Status           string `json:"status"`
}

type RemoveOrphanFilesParameters struct {
	Catalog       string
	Database      string
	Table         string
	RetentionDays int
	OlderThan     time.Time
}

const (
	sparkApplicationTaskIDAnnotation       = "lakehouse-admin.justtrack.io/task-id"
	sparkApplicationTaskKindAnnotation     = "lakehouse-admin.justtrack.io/task-kind"
	sparkApplicationTaskDatabaseAnnotation = "lakehouse-admin.justtrack.io/task-database"
	sparkApplicationTaskTableAnnotation    = "lakehouse-admin.justtrack.io/task-table"
)

const sparkMaintenancePyFile = "maintenance.py"

const (
	sparkProcedureRewriteDataFiles  = "rewrite_data_files"
	sparkProcedureExpireSnapshots   = "expire_snapshots"
	sparkProcedureRemoveOrphanFiles = "remove_orphan_files"
)

const sparkApplicationNameMaxLength = 63

type SparkMaintenanceExecutor struct {
	// taskLocks serializes submission and terminal handling per task id; one
	// pod runs the worker (Recreate), so an in-process lock is enough.
	taskLocks       sync.Map
	logger          log.Logger
	metadata        *ServiceMetadata
	k8s             SparkApplicationCreator
	taskQueue       TaskClaimer
	refresh         *ServiceRefresh
	sqlClient       sqlc.Client
	icebergSettings *IcebergSettings
	settings        *SparkSettings
}

func sparkTaskProcedure(taskKind TaskKind) (string, error) {
	switch taskKind {
	case TaskKindOptimize:
		return sparkProcedureRewriteDataFiles, nil
	case TaskKindExpireSnapshots:
		return sparkProcedureExpireSnapshots, nil
	case TaskKindRemoveOrphanFiles:
		return sparkProcedureRemoveOrphanFiles, nil
	default:
		return "", fmt.Errorf("unknown task kind: %s", taskKind)
	}
}

func NewSparkMaintenanceExecutor(ctx context.Context, config cfg.Config, logger log.Logger) (*SparkMaintenanceExecutor, error) {
	var err error
	var metadata *ServiceMetadata
	var k8s *K8sService
	var taskQueue TaskClaimer
	var refresh *ServiceRefresh
	var sqlClient sqlc.Client
	var icebergSettings *IcebergSettings
	var settings *SparkSettings

	if metadata, err = NewServiceMetadata(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create metadata service: %w", err)
	}

	if k8s, err = ProvideK8sService(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create k8s service: %w", err)
	}

	if taskQueue, err = NewServiceTaskQueue(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create task queue service: %w", err)
	}

	if refresh, err = NewServiceRefresh(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create refresh service: %w", err)
	}

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sql client: %w", err)
	}

	if icebergSettings, err = ReadIcebergSettings(config); err != nil {
		return nil, fmt.Errorf("could not read iceberg settings: %w", err)
	}

	if settings, err = ReadSparkSettings(config); err != nil {
		return nil, fmt.Errorf("could not read spark settings: %w", err)
	}

	return &SparkMaintenanceExecutor{
		logger:          logger.WithChannel("maintenance_executor_spark"),
		metadata:        metadata,
		k8s:             k8s,
		taskQueue:       taskQueue,
		refresh:         refresh,
		sqlClient:       sqlClient,
		icebergSettings: icebergSettings,
		settings:        settings,
	}, nil
}

func (s *SparkMaintenanceExecutor) Engine() TaskEngine {
	return TaskEngineSpark
}

func (s *SparkMaintenanceExecutor) Run(ctx context.Context) error {
	var err error
	var informer cache.SharedIndexInformer

	if informer, err = s.k8s.WatchSparkApplications(ctx); err != nil {
		return fmt.Errorf("could not watch spark applications: %w", err)
	}

	if informer == nil {
		<-ctx.Done()

		return nil
	}

	if _, err = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if err := s.handleSparkApplicationEvent(ctx, obj); err != nil {
				s.logger.Error(ctx, "%s", err)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			if err := s.handleSparkApplicationUpdateEvent(ctx, oldObj, newObj); err != nil {
				s.logger.Error(ctx, "%s", err)
			}
		},
	}); err != nil {
		return fmt.Errorf("could not register spark application event handler: %w", err)
	}

	<-ctx.Done()

	return nil
}

// reconcileClaimGrace is how long a claimed task may run without a spark
// application before it counts as abandoned. Submitting one takes seconds, so
// this leaves an active claim (of any worker) alone.
const reconcileClaimGrace = 15 * time.Minute

// Reconcile settles running tasks that have had no spark application for
// longer than reconcileClaimGrace: their worker stopped after claiming them but
// before submitting one, or during a refresh-only retry. Tasks with an
// application are left to the informer.
func (s *SparkMaintenanceExecutor) Reconcile(ctx context.Context) error {
	var err error
	var tasks []Task
	var submitted map[int64]bool

	if tasks, err = s.taskQueue.ListRunningTasks(ctx, TaskEngineSpark); err != nil || len(tasks) == 0 {
		return err
	}

	if submitted, err = s.k8s.ListSparkApplicationTaskIDs(ctx); err != nil {
		return fmt.Errorf("could not list spark applications to reconcile running tasks: %w", err)
	}

	abandonedBefore := time.Now().Add(-reconcileClaimGrace)
	for i := range tasks {
		pickedUp := tasks[i].PickedUpAt
		if submitted[tasks[i].Id] || (pickedUp != nil && pickedUp.After(abandonedBefore)) {
			continue
		}

		if err := s.reconcileUnsubmittedTask(ctx, &tasks[i]); err != nil {
			return err
		}
	}

	return nil
}

func (s *SparkMaintenanceExecutor) reconcileUnsubmittedTask(ctx context.Context, task *Task) error {
	s.logger.Info(ctx, "requeueing interrupted %s task %d", task.Kind, task.Id)

	return s.taskQueue.RequeueTask(ctx, task.Id)
}

func (s *SparkMaintenanceExecutor) ProcessTask(ctx context.Context, task *Task) error {
	var err error

	input := task.Input.Get()

	switch TaskKind(task.Kind) {
	case TaskKindOptimize:
		err = s.processOptimize(ctx, task, input)
	case TaskKindExpireSnapshots:
		err = s.processExpireSnapshots(ctx, task, input)
	case TaskKindRemoveOrphanFiles:
		err = s.processRemoveOrphanFiles(ctx, task, input)
	default:
		return s.taskQueue.CompleteTask(ctx, task.Id, nil, fmt.Errorf("unknown task kind: %s", task.Kind))
	}

	// Leave the task running: the informer completes it if its application
	// exists, and reconciliation requeues it if it never appears.
	if errors.Is(err, errTaskNoLongerRunning) {
		s.logger.Info(ctx, "not submitting task %d: %s", task.Id, err)

		return nil
	}

	if errors.Is(err, errSparkSubmissionUnknown) {
		s.logger.Warn(ctx, "task %d stays running until its spark application shows up or reconciliation requeues it: %s", task.Id, err)

		return nil
	}

	return err
}

// errTaskNoLongerRunning reports a task that left running before its
// application was submitted.
var errTaskNoLongerRunning = errors.New("task is no longer running")

// errSparkSubmissionUnknown reports a create call that failed without telling
// whether the application was created.
var errSparkSubmissionUnknown = errors.New("could not tell whether the spark application was created")

// submitSparkApplication creates the application. Its name is unique per
// task, so the outcome of an ambiguous create (timeout, connection reset, 5xx)
// is settled under that name: the informer completes the task if the
// application exists, and reconciliation requeues it if it never appears, so
// the retry creates no second job. Only a definitive rejection fails the task.
func (s *SparkMaintenanceExecutor) submitSparkApplication(ctx context.Context, manifest *SparkApplicationManifest, taskID int64) error {
	defer s.lockTask(taskID)()

	// The informer may have completed the task (and deleted its application)
	// since it was claimed; creating it again would run the job twice.
	var err error
	var status string

	if status, err = s.taskQueue.TaskStatus(ctx, taskID); err != nil {
		return fmt.Errorf("%w: could not recheck task %d before submitting: %w", errSparkSubmissionUnknown, taskID, err)
	}

	if status != taskStatusRunning {
		return fmt.Errorf("%w: task %d is %q", errTaskNoLongerRunning, taskID, status)
	}

	_, createErr := s.k8s.CreateSparkApplication(ctx, manifest)
	if createErr == nil {
		return nil
	}

	if !k8serrors.IsAlreadyExists(createErr) && isDefinitiveRejection(createErr) {
		return createErr
	}

	var existing *SparkApplicationManifest

	existing, err = s.k8s.GetSparkApplication(ctx, manifest.Metadata.Namespace, manifest.Metadata.Name)
	switch {
	case err != nil:
		return fmt.Errorf("%w: %w (lookup: %w)", errSparkSubmissionUnknown, createErr, err)
	case existing != nil && existing.Metadata.Annotations[sparkApplicationTaskIDAnnotation] == strconv.FormatInt(taskID, 10):
		s.logger.Warn(ctx, "spark application %s of task %d exists although its create failed: %s", manifest.Metadata.Name, taskID, createErr)

		return nil
	case existing != nil:
		return fmt.Errorf("spark application %s exists for another task: %w", manifest.Metadata.Name, createErr)
	default:
		// The create may still land after this lookup.
		return fmt.Errorf("%w: %w", errSparkSubmissionUnknown, createErr)
	}
}

// isDefinitiveRejection reports an API answer that the create did not happen:
// a 4xx other than a timeout or throttling.
func isDefinitiveRejection(err error) bool {
	var status k8serrors.APIStatus
	if !errors.As(err, &status) {
		return false
	}

	code := status.Status().Code

	return code >= 400 && code < 500 && code != http.StatusRequestTimeout && code != http.StatusTooManyRequests
}

// recordSubmission stores the tracking result of a submitted spark application.
// A failed write must not fail the task: the application already runs, and the
// informer completes the still running task from its task-id annotation, while
// a failed task could be retried and run the procedure twice.
func (s *SparkMaintenanceExecutor) recordSubmission(ctx context.Context, taskID int64, result map[string]any) {
	if err := s.taskQueue.UpdateTaskResult(ctx, taskID, result); err != nil {
		s.logger.Warn(ctx, "could not record the spark application of task %d, the informer completes it anyway: %s", taskID, err)

		return
	}

	s.logger.Info(ctx, "task %d submitted and waiting for asynchronous completion", taskID)
}

func (s *SparkMaintenanceExecutor) processOptimize(ctx context.Context, task *Task, input map[string]any) error {
	var err error
	var res *OptimizeResult

	targetFileSizeMb, _ := input["target_file_size_mb"].(float64)
	from := cast.ToTime(input["from"])
	to := cast.ToTime(input["to"])

	if res, err = s.executeOptimize(ctx, task.Id, task.Database, task.Table, int(targetFileSizeMb), from, to); err != nil {
		return fmt.Errorf("could not execute optimize task: %w", err)
	}

	s.recordSubmission(ctx, task.Id, optimizeResultMap(res))

	return nil
}

func (s *SparkMaintenanceExecutor) processExpireSnapshots(ctx context.Context, task *Task, input map[string]any) error {
	var err error
	var result map[string]any

	retentionDays, _ := input["retention_days"].(float64)

	if result, err = s.executeExpireSnapshots(ctx, task.Id, task.Database, task.Table, int(retentionDays)); err != nil {
		return fmt.Errorf("could not execute expire snapshots task: %w", err)
	}

	s.recordSubmission(ctx, task.Id, result)

	return nil
}

func (s *SparkMaintenanceExecutor) processRemoveOrphanFiles(ctx context.Context, task *Task, input map[string]any) error {
	var err error
	var result map[string]any

	retentionDays, _ := input["retention_days"].(float64)

	if result, err = s.executeRemoveOrphanFiles(ctx, task.Id, task.Database, task.Table, int(retentionDays)); err != nil {
		return fmt.Errorf("could not execute remove orphan files task: %w", err)
	}

	s.recordSubmission(ctx, task.Id, result)

	return nil
}

func (s *SparkMaintenanceExecutor) executeOptimize(ctx context.Context, taskID int64, database string, table string, targetFileSizeMb int, from time.Time, to time.Time) (*OptimizeResult, error) {
	if targetFileSizeMb < 1 {
		return nil, fmt.Errorf("target file size must be at least 1 MB")
	}

	var err error
	var desc *TableDescription
	var partitionColumn string
	var manifest *SparkApplicationManifest

	if from.After(to) {
		return nil, fmt.Errorf("from date must be before or equal to the to date")
	}

	if desc, err = s.metadata.GetTable(ctx, database, table); err != nil {
		return nil, fmt.Errorf("could not get table metadata: %w", err)
	}

	for _, p := range desc.Partitions.Get() {
		if p.IsHidden && p.Hidden.Type == "day" {
			partitionColumn = p.Hidden.Column
		}
	}

	if partitionColumn == "" {
		return nil, fmt.Errorf("no suitable day-partition column found for optimization")
	}

	whereClause := fmt.Sprintf("date(%s) >= date '%s' AND date(%s) <= date '%s'", partitionColumn, from.Format(time.DateOnly), partitionColumn, to.Format(time.DateOnly))
	applicationName := buildSparkApplicationName("rewrite-data-files", table, taskID)

	s.logger.Info(ctx, "creating spark application for table %s range %s to %s", table, from.Format(time.DateOnly), to.Format(time.DateOnly))

	if manifest, err = LoadSparkApplicationTemplate(); err != nil {
		return nil, fmt.Errorf("could not load spark application template: %w", err)
	}

	if err = s.prepareSparkApplication(manifest, TaskKindOptimize, taskID, database, table, applicationName); err != nil {
		return nil, fmt.Errorf("could not prepare spark application manifest: %w", err)
	}

	envValues := map[string]string{
		"ICEBERG_WHERE_COLUMN":               partitionColumn,
		"ICEBERG_WHERE_FROM":                 from.Format(time.DateOnly),
		"ICEBERG_WHERE_UNTIL":                to.Add(time.Hour * 24).Format(time.DateOnly),
		"TARGET_FILE_SIZE_BYTES":             fmt.Sprintf("%d", int64(targetFileSizeMb)*1024*1024),
		"MIN_INPUT_FILES":                    fmt.Sprintf("%d", 2),
		"PARTIAL_PROGRESS_ENABLED":           fmt.Sprintf("%t", s.settings.Optimize.PartialProgressEnabled),
		"PARTIAL_PROGRESS_MAX_COMMITS":       fmt.Sprintf("%d", s.settings.Optimize.PartialProgressMaxCommits),
		"MAX_CONCURRENT_FILE_GROUP_REWRITES": fmt.Sprintf("%d", s.settings.Optimize.MaxConcurrentFileGroupRewrite),
	}

	if err = manifest.SetEnvValues(envValues); err != nil {
		return nil, fmt.Errorf("could not set env values: %w", err)
	}

	if err = s.submitSparkApplication(ctx, manifest, taskID); err != nil {
		return nil, fmt.Errorf("could not create spark application for table %s (range %s): %w", table, whereClause, err)
	}

	return &OptimizeResult{
		Database:         database,
		Table:            table,
		TargetFileSizeMb: targetFileSizeMb,
		Where:            whereClause,
		ApplicationName:  applicationName,
		Status:           statusSubmitted,
	}, nil
}

func (s *SparkMaintenanceExecutor) executeExpireSnapshots(ctx context.Context, taskID int64, database string, table string, retentionDays int) (map[string]any, error) {
	var err error
	var manifest *SparkApplicationManifest

	if retentionDays < 1 {
		return nil, fmt.Errorf("retention days must be at least 1")
	}

	olderThan := time.Now().UTC().AddDate(0, 0, -retentionDays)
	applicationName := buildSparkApplicationName("expire-snapshots", table, taskID)
	s.logger.Info(ctx, "creating spark application to expire snapshots for table %s", table)

	if manifest, err = LoadSparkApplicationTemplate(); err != nil {
		return nil, fmt.Errorf("could not load spark application template: %w", err)
	}

	if err = s.prepareSparkApplication(manifest, TaskKindExpireSnapshots, taskID, database, table, applicationName); err != nil {
		return nil, fmt.Errorf("could not prepare spark application manifest: %w", err)
	}

	envValues := map[string]string{
		"RETENTION_DAYS":         fmt.Sprintf("%d", retentionDays),
		"OLDER_THAN":             olderThan.UTC().Format(time.RFC3339),
		"CLEAN_EXPIRED_METADATA": fmt.Sprintf("%t", true),
	}

	if err = manifest.SetEnvValues(envValues); err != nil {
		return nil, fmt.Errorf("could not set env values: %w", err)
	}

	if err = s.submitSparkApplication(ctx, manifest, taskID); err != nil {
		return nil, fmt.Errorf("could not create spark application to expire snapshots for table %s: %w", table, err)
	}

	return map[string]any{
		"database":               database,
		"table":                  table,
		"retention_days":         retentionDays,
		"older_than":             olderThan,
		"clean_expired_metadata": true,
		"tracking_id":            applicationName,
		"application_name":       applicationName,
		"status":                 statusSubmitted,
	}, nil
}

func (s *SparkMaintenanceExecutor) executeRemoveOrphanFiles(ctx context.Context, taskID int64, database string, table string, retentionDays int) (map[string]any, error) {
	var err error
	var manifest *SparkApplicationManifest

	if retentionDays < 1 {
		return nil, fmt.Errorf("retention days must be at least 1")
	}

	olderThan := time.Now().UTC().AddDate(0, 0, -retentionDays)
	applicationName := buildSparkApplicationName("remove-orphan-files", table, taskID)
	s.logger.Info(ctx, "creating spark application to remove orphan files for table %s", table)

	if manifest, err = LoadSparkApplicationTemplate(); err != nil {
		return nil, fmt.Errorf("could not load spark application template: %w", err)
	}

	if err = s.prepareSparkApplication(manifest, TaskKindRemoveOrphanFiles, taskID, database, table, applicationName); err != nil {
		return nil, fmt.Errorf("could not prepare spark application manifest: %w", err)
	}

	envValues := map[string]string{
		"RETENTION_DAYS": fmt.Sprintf("%d", retentionDays),
		"OLDER_THAN":     olderThan.UTC().Format(time.RFC3339),
	}

	if err = manifest.SetEnvValues(envValues); err != nil {
		return nil, fmt.Errorf("could not set env values: %w", err)
	}

	if err = s.submitSparkApplication(ctx, manifest, taskID); err != nil {
		return nil, fmt.Errorf("could not create spark application to remove orphan files for table %s: %w", table, err)
	}

	return map[string]any{
		"database":         database,
		"table":            table,
		"retention_days":   retentionDays,
		"older_than":       olderThan,
		"tracking_id":      applicationName,
		"application_name": applicationName,
		"status":           statusSubmitted,
	}, nil
}

func (s *SparkMaintenanceExecutor) prepareSparkApplication(manifest *SparkApplicationManifest, taskKind TaskKind, taskID int64, database string, table string, applicationName string) error {
	var err error
	var procedure string

	if procedure, err = sparkTaskProcedure(taskKind); err != nil {
		return fmt.Errorf("could not determine spark task procedure: %w", err)
	}

	manifest.Metadata.Name = applicationName
	manifest.SetAnnotation(sparkApplicationTaskIDAnnotation, strconv.FormatInt(taskID, 10))
	manifest.SetAnnotation(sparkApplicationTaskKindAnnotation, string(taskKind))
	manifest.SetAnnotation(sparkApplicationTaskDatabaseAnnotation, database)
	manifest.SetAnnotation(sparkApplicationTaskTableAnnotation, table)
	manifest.MergeDriverPodAnnotations(s.settings.DriverPodSpec.Annotations)
	manifest.MergeDriverNodeSelector(s.settings.DriverPodSpec.NodeSelector)
	manifest.AppendDriverTolerations(s.settings.DriverPodSpec.Tolerations)
	manifest.MergeExecutorPodAnnotations(s.settings.ExecutorPodSpec.Annotations)
	manifest.MergeExecutorNodeSelector(s.settings.ExecutorPodSpec.NodeSelector)
	manifest.AppendExecutorTolerations(s.settings.ExecutorPodSpec.Tolerations)

	if err := manifest.SetPyFileName(sparkMaintenancePyFile); err != nil {
		return fmt.Errorf("could not set spark application pyFiles: %w", err)
	}

	return manifest.SetEnvValues(map[string]string{
		"ICEBERG_CATALOG":       s.icebergSettings.Catalog,
		"ICEBERG_DATABASE":      database,
		"ICEBERG_TABLE":         table,
		"TASK_CALLBACK_ENABLED": fmt.Sprintf("%t", s.settings.Callback.Enabled),
		"TASK_CALLBACK_URL":     BuildTaskProcedureCallbackURL(s.settings.Callback.BackendHost, taskID),
		"TASK_PROCEDURE":        procedure,
		"TASK_ID":               strconv.FormatInt(taskID, 10),
	})
}

// HandleTaskUpdate completes a task from its spark application's terminal state.
// HandleTaskUpdate completes a running task from its application's terminal
// state and reports whether it did.
func (s *SparkMaintenanceExecutor) HandleTaskUpdate(ctx context.Context, taskID int64, applicationName string, state string, message string, extraResult map[string]any) (bool, error) {
	var taskErr error

	result := map[string]any{
		"tracking_id":      applicationName,
		"application_name": applicationName,
		"spark_state":      state,
		"status":           statusOK,
	}

	for key, value := range extraResult {
		result[key] = value
	}

	if !isSparkApplicationSuccessState(state) {
		if message == "" {
			message = fmt.Sprintf("spark application %s finished with state %s", applicationName, state)
		}
		result["status"] = statusError
		taskErr = fmt.Errorf("%s", message)
	}

	var err error
	var completed bool

	if completed, err = s.taskQueue.CompleteRunningTask(ctx, taskID, result, taskErr); err != nil {
		return false, fmt.Errorf("could not complete task %d from spark application %s: %w", taskID, applicationName, err)
	}

	if completed {
		s.logger.Info(ctx, "completed task %d from spark application %s with state %s", taskID, applicationName, state)
	}

	return completed, nil
}

// completeFromApplication completes the running task from its terminal
// application and returns the task's status afterwards.
func (s *SparkMaintenanceExecutor) completeFromApplication(ctx context.Context, taskID int64, manifest *SparkApplicationManifest, resolved ResolvedSparkApplicationStatus, extraResult map[string]any) (string, error) {
	var err error
	var completed bool

	appName := manifest.Metadata.Name
	if resolved.IsSuccess() {
		s.refreshAfterSuccess(ctx, manifest.Metadata.Annotations)
	}

	if completed, err = s.HandleTaskUpdate(ctx, taskID, appName, resolved.State(), resolved.Message, extraResult); err != nil {
		return "", fmt.Errorf("could not resolve spark tracking update for %s: %w", appName, err)
	}

	if completed {
		return taskStatusSuccess, nil
	}

	// The row left running meanwhile (requeued, say): decide from its status.
	return s.taskQueue.TaskStatus(ctx, taskID)
}

// lockTask holds the task's lock until the returned function is called.
func (s *SparkMaintenanceExecutor) lockTask(taskID int64) func() {
	lock, _ := s.taskLocks.LoadOrStore(taskID, &sync.Mutex{})
	mutex := lock.(*sync.Mutex)
	mutex.Lock()

	return mutex.Unlock
}

func (s *SparkMaintenanceExecutor) handleSparkApplicationEvent(ctx context.Context, obj any) error {
	var err error
	var manifest *SparkApplicationManifest

	if manifest, err = decodeSparkApplicationEvent(obj); err != nil {
		return err
	}

	return s.handleDecodedSparkApplicationEvent(ctx, manifest)
}

func (s *SparkMaintenanceExecutor) handleSparkApplicationUpdateEvent(ctx context.Context, oldObj any, newObj any) error {
	var err error
	var oldManifest, newManifest *SparkApplicationManifest

	if newManifest, err = decodeSparkApplicationEvent(newObj); err != nil {
		return fmt.Errorf("could not decode updated spark application event: %w", err)
	}

	if oldManifest, err = decodeSparkApplicationEvent(oldObj); err != nil {
		return fmt.Errorf("could not decode previous spark application event: %w", err)
	}

	if !shouldHandleSparkApplicationUpdate(oldManifest.Status, newManifest.Status) {
		return nil
	}

	return s.handleDecodedSparkApplicationEvent(ctx, newManifest)
}

func (s *SparkMaintenanceExecutor) handleDecodedSparkApplicationEvent(ctx context.Context, manifest *SparkApplicationManifest) error {
	var ok bool
	var err error
	var taskIDAnnotation string
	var taskID int64
	var status string

	appName := manifest.Metadata.Name
	resolvedStatus := manifest.Status.Resolve()
	extraResult := map[string]any{
		"spark_current_state": resolvedStatus.CurrentState,
	}

	if transitions := manifest.Status.TransitionResults(); len(transitions) > 0 {
		extraResult["spark_state_transitions"] = transitions
	}

	if !resolvedStatus.IsTerminal() {
		return nil
	}

	if taskIDAnnotation, ok = manifest.Metadata.Annotations[sparkApplicationTaskIDAnnotation]; !ok || taskIDAnnotation == "" {
		return fmt.Errorf("ignoring terminal spark application event for %s without %s annotation", appName, sparkApplicationTaskIDAnnotation)
	}

	if taskID, err = strconv.ParseInt(taskIDAnnotation, 10, 64); err != nil {
		return fmt.Errorf("ignoring terminal spark application event for %s with invalid %s annotation %q: %w", appName, sparkApplicationTaskIDAnnotation, taskIDAnnotation, err)
	}

	defer s.lockTask(taskID)()

	// Informer resyncs replay terminal applications, so a completion that failed
	// to write is retried while its task is still running.
	if status, err = s.taskQueue.TaskStatus(ctx, taskID); err != nil {
		return fmt.Errorf("could not load task %d of spark application %s: %w", taskID, appName, err)
	}

	if status == taskStatusRunning {
		if status, err = s.completeFromApplication(ctx, taskID, manifest, resolvedStatus, extraResult); err != nil {
			return err
		}
	}

	// Clean up after a success the task records, or after a task that no longer
	// exists; failed applications stay for inspection. A queued or running
	// task's application stays: its next claim adopts it instead of
	// submitting the job again.
	if status == "" || (resolvedStatus.IsSuccess() && (status == taskStatusSuccess || status == taskStatusError)) {
		return s.deleteSparkApplication(ctx, manifest, taskID)
	}

	return nil
}

func (s *SparkMaintenanceExecutor) deleteSparkApplication(ctx context.Context, manifest *SparkApplicationManifest, taskID int64) error {
	if err := s.k8s.DeleteSparkApplication(ctx, manifest.Metadata.Namespace, manifest.Metadata.Name); err != nil {
		return fmt.Errorf("could not delete spark application %s of task %d: %w", manifest.Metadata.Name, taskID, err)
	}

	s.logger.Info(ctx, "deleted spark application %s of task %d", manifest.Metadata.Name, taskID)

	return nil
}

// refreshAfterSuccess reloads the snapshots an expire task removed, as the
// Trino executor did. The scheduled refresh catches up if this fails.
func (s *SparkMaintenanceExecutor) refreshAfterSuccess(ctx context.Context, annotations map[string]string) {
	database := annotations[sparkApplicationTaskDatabaseAnnotation]
	table := annotations[sparkApplicationTaskTableAnnotation]

	if TaskKind(annotations[sparkApplicationTaskKindAnnotation]) != TaskKindExpireSnapshots || database == "" || table == "" {
		return
	}

	err := s.sqlClient.WithTx(ctx, func(cttx sqlc.Tx) error {
		_, err := s.refresh.RefreshSnapshots(cttx, database, table)

		return err
	})
	if err != nil {
		s.logger.Warn(ctx, "could not refresh snapshots of %s.%s after expiring them: %s", database, table, err)
	}
}

func buildSparkApplicationName(prefix string, table string, taskID int64) string {
	tablePart := sanitizeK8sName(table)
	suffix := strconv.FormatInt(taskID, 10)
	maxTableLength := sparkApplicationNameMaxLength - len(prefix) - len(suffix) - 2

	if maxTableLength <= 0 {
		return prefix + "-" + suffix
	}

	if len(tablePart) > maxTableLength {
		tablePart = strings.Trim(tablePart[:maxTableLength], "-")
	}

	if tablePart == "" || tablePart == sparkApplicationDefaultName {
		return prefix + "-" + suffix
	}

	return prefix + "-" + tablePart + "-" + suffix
}

func decodeSparkApplicationEvent(obj any) (*SparkApplicationManifest, error) {
	var err error
	var manifest *SparkApplicationManifest

	resource, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("ignoring unexpected spark application event type %T", obj)
	}

	if manifest, err = UnstructuredToSparkApplicationManifest(resource); err != nil {
		return nil, fmt.Errorf("could not decode spark application event for %s: %w", resource.GetName(), err)
	}

	return manifest, nil
}

func shouldHandleSparkApplicationUpdate(_ SparkApplicationStatus, newStatus SparkApplicationStatus) bool {
	return newStatus.Resolve().IsTerminal()
}

func isSparkApplicationTerminalState(state string) bool {
	return isSparkApplicationSuccessState(state) || isSparkApplicationFailureState(state) || isSparkApplicationTerminalWrapperState(state)
}

func isSparkApplicationSuccessState(state string) bool {
	switch normalizeSparkApplicationState(state) {
	case "COMPLETED", "SUCCEEDED":
		return true
	default:
		return false
	}
}

func isSparkApplicationFailureState(state string) bool {
	switch normalizeSparkApplicationState(state) {
	case "FAILED", "SUBMISSIONFAILED", "SUBMISSION_FAILED", "FAILING":
		return true
	default:
		return false
	}
}

func isSparkApplicationTerminalWrapperState(state string) bool {
	switch normalizeSparkApplicationState(state) {
	case "TERMINATEDWITHOUTRELEASERESOURCES", "RESOURCERELEASED":
		return true
	default:
		return false
	}
}

func normalizeSparkApplicationState(state string) string {
	return strings.ToUpper(strings.TrimSpace(state))
}
