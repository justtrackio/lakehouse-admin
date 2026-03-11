package internal

import (
	"context"
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

const sparkApplicationTaskIDAnnotation = "lakehouse-admin.justtrack.io/task-id"

type SparkMaintenanceExecutor struct {
	logger    log.Logger
	metadata  *ServiceMetadata
	k8s       SparkApplicationCreator
	taskQueue TaskClaimer
}

func NewSparkMaintenanceExecutor(ctx context.Context, config cfg.Config, logger log.Logger) (*SparkMaintenanceExecutor, error) {
	var err error
	var metadata *ServiceMetadata
	var k8s *K8sService
	var taskQueue TaskClaimer

	if metadata, err = NewServiceMetadata(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create metadata service: %w", err)
	}

	if k8s, err = ProvideK8sService(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create k8s service: %w", err)
	}

	if taskQueue, err = NewServiceTaskQueue(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create task queue service: %w", err)
	}

	return &SparkMaintenanceExecutor{
		logger:    logger.WithChannel("maintenance_executor_spark"),
		metadata:  metadata,
		k8s:       k8s,
		taskQueue: taskQueue,
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
			s.handleSparkApplicationEvent(ctx, obj)
		},
		UpdateFunc: func(oldObj, newObj any) {
			s.handleSparkApplicationUpdateEvent(ctx, oldObj, newObj)
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
		return s.taskQueue.CompleteTask(ctx, task.Id, nil, fmt.Errorf("task kind %s is not supported by engine %s", TaskKind(task.Kind), s.Engine()))
	case TaskKindRemoveOrphanFiles:
		return s.taskQueue.CompleteTask(ctx, task.Id, nil, fmt.Errorf("task kind %s is not supported by engine %s", TaskKind(task.Kind), s.Engine()))
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
	applicationName := fmt.Sprintf("rewrite-data-files-%s-%s", table, from.Format("2006-01-02"))

	s.logger.Info(ctx, "creating spark application for table %s range %s to %s", table, from.Format(time.DateOnly), to.Format(time.DateOnly))

	if manifest, err = LoadRewriteDataFilesTemplate(); err != nil {
		return nil, fmt.Errorf("could not load spark application template: %w", err)
	}

	manifest.Metadata.Name = sanitizeK8sName(applicationName)
	manifest.SetAnnotation(sparkApplicationTaskIDAnnotation, strconv.FormatInt(taskID, 10))

	if err = manifest.ApplyValues(RewriteDataFilesParameters{
		Catalog:                   "lakehouse",
		Database:                  "main",
		Table:                     table,
		WhereColumn:               partitionColumn,
		WhereFrom:                 from,
		WhereUntil:                to.Add(time.Hour * 24),
		TargetFileSizeBytes:       int64(fileSizeThresholdMb) * 1024 * 1024,
		MinInputFiles:             2,
		PartialProgressEnabled:    true,
		PartialProgressMaxCommits: 10,
	}); err != nil {
		return nil, fmt.Errorf("could not apply spark application values: %w", err)
	}

	if _, err = s.k8s.CreateSparkApplication(ctx, manifest); err != nil {
		return nil, fmt.Errorf("could not create spark application for table %s (range %s): %w", table, whereClause, err)
	}

	return &OptimizeResult{
		Table:               table,
		FileSizeThresholdMb: fileSizeThresholdMb,
		Where:               whereClause,
		ApplicationName:     applicationName,
		Status:              "submitted",
	}, nil
}

func (s *SparkMaintenanceExecutor) HandleTaskUpdate(ctx context.Context, taskID int64, applicationName string, state string, message string, extraResult map[string]any) error {
	var taskErr error

	result := map[string]any{
		"tracking_id":      applicationName,
		"application_name": applicationName,
		"spark_state":      state,
		"status":           "ok",
	}

	for key, value := range extraResult {
		result[key] = value
	}

	if !isSparkApplicationSuccessState(state) {
		if message == "" {
			message = fmt.Sprintf("spark application %s finished with state %s", applicationName, state)
		}
		result["status"] = "error"
		taskErr = fmt.Errorf("%s", message)
	}

	if err := s.taskQueue.CompleteTask(ctx, taskID, result, taskErr); err != nil {
		return fmt.Errorf("could not complete task %d from spark application %s: %w", taskID, applicationName, err)
	}

	s.logger.Info(ctx, "completed task %d from spark application %s with state %s", taskID, applicationName, state)

	return nil
}

func (s *SparkMaintenanceExecutor) handleSparkApplicationEvent(ctx context.Context, obj any) {
	manifest, err := decodeSparkApplicationEvent(obj)
	if err != nil {
		s.logger.Error(ctx, "%s", err)

		return
	}

	s.handleDecodedSparkApplicationEvent(ctx, manifest)
}

func (s *SparkMaintenanceExecutor) handleSparkApplicationUpdateEvent(ctx context.Context, oldObj any, newObj any) {
	var err error
	var oldManifest, newManifest *SparkApplicationManifest

	if newManifest, err = decodeSparkApplicationEvent(newObj); err != nil {
		s.logger.Error(ctx, "%s", err)

		return
	}

	if oldManifest, err = decodeSparkApplicationEvent(oldObj); err != nil {
		s.logger.Error(ctx, "%s", err)

		return
	}

	if !shouldHandleSparkApplicationUpdate(oldManifest.Status, newManifest.Status) {
		return
	}

	s.handleDecodedSparkApplicationEvent(ctx, newManifest)
}

func (s *SparkMaintenanceExecutor) handleDecodedSparkApplicationEvent(ctx context.Context, manifest *SparkApplicationManifest) {
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
		return
	}

	if taskIDAnnotation, ok = manifest.Metadata.Annotations[sparkApplicationTaskIDAnnotation]; !ok || taskIDAnnotation == "" {
		s.logger.Error(ctx, "ignoring terminal spark application event for %s without %s annotation", appName, sparkApplicationTaskIDAnnotation)

		return
	}

	if taskID, err = strconv.ParseInt(taskIDAnnotation, 10, 64); err != nil {
		s.logger.Error(ctx, "ignoring terminal spark application event for %s with invalid %s annotation %q: %s", appName, sparkApplicationTaskIDAnnotation, taskIDAnnotation, err)

		return
	}

	if err = s.HandleTaskUpdate(ctx, taskID, appName, state, resolvedStatus.Message, extraResult); err != nil {
		s.logger.Error(ctx, "could not resolve spark tracking update for %s: %s", appName, err)

		return
	}
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
