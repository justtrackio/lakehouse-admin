package internal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/spf13/cast"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

type OptimizeResult struct {
	Table               string `json:"table"`
	FileSizeThresholdMb int    `json:"file_size_threshold_mb"`
	Where               string `json:"where"`
	ApplicationName     string `json:"application_name"`
	Status              string `json:"status"`
}

type RemoveOrphanFilesParameters struct {
	Catalog       string
	Database      string
	Table         string
	RetentionDays int
	OlderThan     time.Time
}

const (
	sparkApplicationTaskIDAnnotation    = "lakehouse-admin.justtrack.io/task-id"
	sparkApplicationTaskKindAnnotation  = "lakehouse-admin.justtrack.io/task-kind"
	sparkApplicationTaskTableAnnotation = "lakehouse-admin.justtrack.io/task-table"
)

const sparkMaintenancePyFile = "maintenance.py"

const (
	sparkProcedureRewriteDataFiles  = "rewrite_data_files"
	sparkProcedureExpireSnapshots   = "expire_snapshots"
	sparkProcedureRemoveOrphanFiles = "remove_orphan_files"
)

const sparkApplicationNameMaxLength = 63

type SparkMaintenanceExecutor struct {
	logger           log.Logger
	metadata         *ServiceMetadata
	k8s              SparkApplicationCreator
	taskQueue        TaskClaimer
	icebergSettings  *IcebergSettings
	callback         *TaskSparkCallbackSettings
	optimizeSettings *TaskSparkOptimizeSettings
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
	var icebergSettings *IcebergSettings
	var callbackSettings *TaskSparkCallbackSettings
	var optimizeSettings *TaskSparkOptimizeSettings

	if metadata, err = NewServiceMetadata(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create metadata service: %w", err)
	}

	if k8s, err = ProvideK8sService(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create k8s service: %w", err)
	}

	if taskQueue, err = NewServiceTaskQueue(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create task queue service: %w", err)
	}

	if icebergSettings, err = ReadIcebergSettings(config); err != nil {
		return nil, fmt.Errorf("could not read iceberg settings: %w", err)
	}

	if callbackSettings, err = ReadTaskSparkCallbackSettings(config); err != nil {
		return nil, fmt.Errorf("could not read spark callback settings: %w", err)
	}

	if optimizeSettings, err = ReadTaskSparkOptimizeSettings(config); err != nil {
		return nil, fmt.Errorf("could not read spark optimize settings: %w", err)
	}

	return &SparkMaintenanceExecutor{
		logger:           logger.WithChannel("maintenance_executor_spark"),
		metadata:         metadata,
		k8s:              k8s,
		taskQueue:        taskQueue,
		icebergSettings:  icebergSettings,
		callback:         callbackSettings,
		optimizeSettings: optimizeSettings,
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

func (s *SparkMaintenanceExecutor) ProcessTask(ctx context.Context, task *Task) error {
	input := task.Input.Get()

	switch TaskKind(task.Kind) {
	case TaskKindOptimize:
		return s.processOptimize(ctx, task, input)
	case TaskKindExpireSnapshots:
		return s.processExpireSnapshots(ctx, task, input)
	case TaskKindRemoveOrphanFiles:
		return s.processRemoveOrphanFiles(ctx, task, input)
	default:
		return s.taskQueue.CompleteTask(ctx, task.Id, nil, fmt.Errorf("unknown task kind: %s", task.Kind))
	}
}

func (s *SparkMaintenanceExecutor) processOptimize(ctx context.Context, task *Task, input map[string]any) error {
	fileSizeThresholdMb, _ := input["file_size_threshold_mb"].(float64)
	from := cast.ToTime(input["from"])
	to := cast.ToTime(input["to"])

	res, err := s.executeOptimize(ctx, task.Id, task.Table, int(fileSizeThresholdMb), from, to)
	if err != nil {
		return fmt.Errorf("could not execute optimize task: %w", err)
	}

	result := optimizeResultMap(res)
	if err = s.taskQueue.UpdateTaskResult(ctx, task.Id, result); err != nil {
		return fmt.Errorf("could not update task %d tracking result: %w", task.Id, err)
	}

	s.logger.Info(ctx, "task %d submitted and waiting for asynchronous completion", task.Id)

	return nil
}

func (s *SparkMaintenanceExecutor) processExpireSnapshots(ctx context.Context, task *Task, input map[string]any) error {
	retentionDays, _ := input["retention_days"].(float64)

	result, err := s.executeExpireSnapshots(ctx, task.Id, task.Table, int(retentionDays))
	if err != nil {
		return fmt.Errorf("could not execute expire snapshots task: %w", err)
	}

	if err = s.taskQueue.UpdateTaskResult(ctx, task.Id, result); err != nil {
		return fmt.Errorf("could not update task %d tracking result: %w", task.Id, err)
	}

	s.logger.Info(ctx, "task %d submitted and waiting for asynchronous completion", task.Id)

	return nil
}

func (s *SparkMaintenanceExecutor) processRemoveOrphanFiles(ctx context.Context, task *Task, input map[string]any) error {
	retentionDays, _ := input["retention_days"].(float64)

	result, err := s.executeRemoveOrphanFiles(ctx, task.Id, task.Table, int(retentionDays))
	if err != nil {
		return fmt.Errorf("could not execute remove orphan files task: %w", err)
	}

	if err = s.taskQueue.UpdateTaskResult(ctx, task.Id, result); err != nil {
		return fmt.Errorf("could not update task %d tracking result: %w", task.Id, err)
	}

	s.logger.Info(ctx, "task %d submitted and waiting for asynchronous completion", task.Id)

	return nil
}

func (s *SparkMaintenanceExecutor) executeOptimize(ctx context.Context, taskID int64, table string, fileSizeThresholdMb int, from time.Time, to time.Time) (*OptimizeResult, error) {
	if fileSizeThresholdMb < 1 {
		return nil, fmt.Errorf("file size threshold must be at least 1")
	}

	var err error
	var desc *TableDescription
	var partitionColumn string
	var manifest *SparkApplicationManifest

	if from.After(to) {
		return nil, fmt.Errorf("from date must be before or equal to the to date")
	}

	if desc, err = s.metadata.GetTable(ctx, table); err != nil {
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

	if err = s.prepareSparkApplication(manifest, TaskKindOptimize, taskID, table, applicationName); err != nil {
		return nil, fmt.Errorf("could not prepare spark application manifest: %w", err)
	}

	envValues := map[string]string{
		"ICEBERG_WHERE_COLUMN":               partitionColumn,
		"ICEBERG_WHERE_FROM":                 from.Format(time.DateOnly),
		"ICEBERG_WHERE_UNTIL":                to.Add(time.Hour * 24).Format(time.DateOnly),
		"TARGET_FILE_SIZE_BYTES":             fmt.Sprintf("%d", int64(fileSizeThresholdMb)*1024*1024),
		"MIN_INPUT_FILES":                    fmt.Sprintf("%d", 2),
		"PARTIAL_PROGRESS_ENABLED":           fmt.Sprintf("%t", s.optimizeSettings.PartialProgressEnabled),
		"PARTIAL_PROGRESS_MAX_COMMITS":       fmt.Sprintf("%d", s.optimizeSettings.PartialProgressMaxCommits),
		"MAX_CONCURRENT_FILE_GROUP_REWRITES": fmt.Sprintf("%d", s.optimizeSettings.MaxConcurrentFileGroupRewrite),
	}

	if err = manifest.SetEnvValues(envValues); err != nil {
		return nil, fmt.Errorf("could not set env values: %w", err)
	}

	if _, err = s.k8s.CreateSparkApplication(ctx, manifest); err != nil {
		return nil, fmt.Errorf("could not create spark application for table %s (range %s): %w", table, whereClause, err)
	}

	return &OptimizeResult{
		Table:               table,
		FileSizeThresholdMb: fileSizeThresholdMb,
		Where:               whereClause,
		ApplicationName:     applicationName,
		Status:              statusSubmitted,
	}, nil
}

func (s *SparkMaintenanceExecutor) executeExpireSnapshots(ctx context.Context, taskID int64, table string, retentionDays int) (map[string]any, error) {
	if retentionDays < 1 {
		return nil, fmt.Errorf("retention days must be at least 1")
	}

	olderThan := time.Now().UTC().AddDate(0, 0, -retentionDays)
	applicationName := buildSparkApplicationName("expire-snapshots", table, taskID)
	s.logger.Info(ctx, "creating spark application to expire snapshots for table %s", table)

	manifest, err := LoadSparkApplicationTemplate()
	if err != nil {
		return nil, fmt.Errorf("could not load spark application template: %w", err)
	}

	if err = s.prepareSparkApplication(manifest, TaskKindExpireSnapshots, taskID, table, applicationName); err != nil {
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

	if _, err = s.k8s.CreateSparkApplication(ctx, manifest); err != nil {
		return nil, fmt.Errorf("could not create spark application to expire snapshots for table %s: %w", table, err)
	}

	return map[string]any{
		"table":                  table,
		"retention_days":         retentionDays,
		"older_than":             olderThan,
		"clean_expired_metadata": true,
		"tracking_id":            applicationName,
		"application_name":       applicationName,
		"status":                 statusSubmitted,
	}, nil
}

func (s *SparkMaintenanceExecutor) executeRemoveOrphanFiles(ctx context.Context, taskID int64, table string, retentionDays int) (map[string]any, error) {
	if retentionDays < 1 {
		return nil, fmt.Errorf("retention days must be at least 1")
	}

	olderThan := time.Now().UTC().AddDate(0, 0, -retentionDays)
	applicationName := buildSparkApplicationName("remove-orphan-files", table, taskID)
	s.logger.Info(ctx, "creating spark application to remove orphan files for table %s", table)

	manifest, err := LoadSparkApplicationTemplate()
	if err != nil {
		return nil, fmt.Errorf("could not load spark application template: %w", err)
	}

	if err = s.prepareSparkApplication(manifest, TaskKindRemoveOrphanFiles, taskID, table, applicationName); err != nil {
		return nil, fmt.Errorf("could not prepare spark application manifest: %w", err)
	}

	envValues := map[string]string{
		"RETENTION_DAYS": fmt.Sprintf("%d", retentionDays),
		"OLDER_THAN":     olderThan.UTC().Format(time.RFC3339),
	}

	if err = manifest.SetEnvValues(envValues); err != nil {
		return nil, fmt.Errorf("could not set env values: %w", err)
	}

	if _, err = s.k8s.CreateSparkApplication(ctx, manifest); err != nil {
		return nil, fmt.Errorf("could not create spark application to remove orphan files for table %s: %w", table, err)
	}

	return map[string]any{
		"table":            table,
		"retention_days":   retentionDays,
		"older_than":       olderThan,
		"tracking_id":      applicationName,
		"application_name": applicationName,
		"status":           statusSubmitted,
	}, nil
}

func (s *SparkMaintenanceExecutor) prepareSparkApplication(manifest *SparkApplicationManifest, taskKind TaskKind, taskID int64, table string, applicationName string) error {
	procedure, err := sparkTaskProcedure(taskKind)
	if err != nil {
		return fmt.Errorf("could not determine spark task procedure: %w", err)
	}

	manifest.Metadata.Name = applicationName
	manifest.SetAnnotation(sparkApplicationTaskIDAnnotation, strconv.FormatInt(taskID, 10))
	manifest.SetAnnotation(sparkApplicationTaskKindAnnotation, string(taskKind))
	manifest.SetAnnotation(sparkApplicationTaskTableAnnotation, table)

	if err := manifest.SetPyFileName(sparkMaintenancePyFile); err != nil {
		return fmt.Errorf("could not set spark application pyFiles: %w", err)
	}

	return manifest.SetEnvValues(map[string]string{
		"ICEBERG_CATALOG":       s.icebergSettings.Catalog,
		"ICEBERG_DATABASE":      s.icebergSettings.Database,
		"ICEBERG_TABLE":         table,
		"TASK_CALLBACK_ENABLED": fmt.Sprintf("%t", s.callback.Enabled),
		"TASK_PROCEDURE":        procedure,
		"TASK_ID":               strconv.FormatInt(taskID, 10),
		"TASK_CALLBACK_URL":     BuildTaskProcedureCallbackURL(s.callback.BackendHost, taskID),
	})
}

func (s *SparkMaintenanceExecutor) HandleTaskUpdate(ctx context.Context, taskID int64, applicationName string, state string, message string, extraResult map[string]any) error {
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

	if err := s.taskQueue.CompleteTask(ctx, taskID, result, taskErr); err != nil {
		return fmt.Errorf("could not complete task %d from spark application %s: %w", taskID, applicationName, err)
	}

	s.logger.Info(ctx, "completed task %d from spark application %s with state %s", taskID, applicationName, state)

	return nil
}

func (s *SparkMaintenanceExecutor) handleSparkApplicationEvent(ctx context.Context, obj any) error {
	manifest, err := decodeSparkApplicationEvent(obj)
	if err != nil {
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

	appName := manifest.Metadata.Name
	resolvedStatus := manifest.Status.Resolve()
	state := resolvedStatus.State()
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

	if err = s.HandleTaskUpdate(ctx, taskID, appName, state, resolvedStatus.Message, extraResult); err == nil {
		if !resolvedStatus.IsSuccess() {
			return nil
		}

		if deleteErr := s.k8s.DeleteSparkApplication(ctx, manifest.Metadata.Namespace, appName); deleteErr != nil {
			return fmt.Errorf("task %d from successful spark application %s could not be cleaned up: %w", taskID, appName, deleteErr)
		}

		s.logger.Info(ctx, "deleted successful spark application %s after completing task %d", appName, taskID)

		return nil
	}

	if errors.Is(err, errTaskCompletionNotFound) {
		if deleteErr := s.k8s.DeleteSparkApplication(ctx, manifest.Metadata.Namespace, appName); deleteErr != nil {
			return fmt.Errorf("task %d for terminal spark application %s no longer exists and could not delete orphaned spark application: %w", taskID, appName, deleteErr)
		}

		return nil
	}

	return fmt.Errorf("could not resolve spark tracking update for %s: %w", appName, err)
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
	resource, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("ignoring unexpected spark application event type %T", obj)
	}

	manifest, err := UnstructuredToSparkApplicationManifest(resource)
	if err != nil {
		return nil, fmt.Errorf("could not decode spark application event for %s: %w", resource.GetName(), err)
	}

	return manifest, nil
}

func shouldHandleSparkApplicationUpdate(oldStatus SparkApplicationStatus, newStatus SparkApplicationStatus) bool {
	newResolved := newStatus.Resolve()
	if !newResolved.IsTerminal() {
		return false
	}

	oldResolved := oldStatus.Resolve()
	if !oldResolved.IsTerminal() {
		return true
	}

	return normalizeSparkApplicationState(oldResolved.State()) != normalizeSparkApplicationState(newResolved.State())
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
