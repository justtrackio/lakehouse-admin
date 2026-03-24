package internal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

const (
	minRetentionDays   = 7
	optimizeChunkDay   = "daily"
	optimizeChunkWeek  = "weekly"
	optimizeChunkMonth = "monthly"
)

type optimizeRangeChunk struct {
	from time.Time
	to   time.Time
}

type ServiceTasks struct {
	logger           log.Logger
	serviceTaskQueue *ServiceTaskQueue
	engineResolver   *TaskEngineResolver
	sqlClient        sqlc.Client
}

type TaskProcedureCallback struct {
	Query      string           `json:"query"`
	Rows       []map[string]any `json:"rows"`
	ReceivedAt DateTime         `json:"received_at"`
	Meta       map[string]any   `json:"meta,omitempty"`
}

type BatchEnqueueFailure struct {
	Table string `json:"table"`
	Error string `json:"error"`
}

type BatchEnqueueResult struct {
	TaskIds       []int64               `json:"task_ids"`
	EnqueuedCount int64                 `json:"enqueued_count"`
	FailedTables  []BatchEnqueueFailure `json:"failed_tables"`
}

type BatchOptimizeTable struct {
	Table   string
	ChunkBy string
}

func NewServiceTasks(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceTasks, error) {
	var err error
	var serviceTaskQueue *ServiceTaskQueue
	var engineResolver *TaskEngineResolver

	var sqlClient sqlc.Client

	if serviceTaskQueue, err = NewServiceTaskQueue(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create task queue service: %w", err)
	}

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sql client: %w", err)
	}

	if engineResolver, err = NewTaskEngineResolver(config); err != nil {
		return nil, fmt.Errorf("could not create task engine resolver: %w", err)
	}

	return &ServiceTasks{
		logger:           logger.WithChannel("tasks"),
		serviceTaskQueue: serviceTaskQueue,
		engineResolver:   engineResolver,
		sqlClient:        sqlClient,
	}, nil
}

// EnqueueExpireSnapshots enqueues a task to expire old snapshots for a table
func (s *ServiceTasks) EnqueueExpireSnapshots(ctx context.Context, table string, retentionDays int) (int64, error) {
	// Apply minimum constraints
	if retentionDays < minRetentionDays {
		retentionDays = minRetentionDays
	}

	taskInput := map[string]any{
		"retention_days": retentionDays,
	}

	engine, err := s.engineResolver.Resolve(TaskKindExpireSnapshots)
	if err != nil {
		return 0, fmt.Errorf("could not resolve engine for expire snapshots task: %w", err)
	}

	taskId, err := s.serviceTaskQueue.EnqueueTask(ctx, table, string(TaskKindExpireSnapshots), string(engine), taskInput)
	if err != nil {
		return 0, fmt.Errorf("could not enqueue expire snapshots task: %w", err)
	}

	return taskId, nil
}

// EnqueueRemoveOrphanFiles enqueues a task to remove orphan files for a table
func (s *ServiceTasks) EnqueueRemoveOrphanFiles(ctx context.Context, table string, retentionDays int) (int64, error) {
	// Apply minimum constraint
	if retentionDays < minRetentionDays {
		retentionDays = minRetentionDays
	}

	taskInput := map[string]any{
		"retention_days": retentionDays,
	}

	engine, err := s.engineResolver.Resolve(TaskKindRemoveOrphanFiles)
	if err != nil {
		return 0, fmt.Errorf("could not resolve engine for remove orphan files task: %w", err)
	}

	taskId, err := s.serviceTaskQueue.EnqueueTask(ctx, table, string(TaskKindRemoveOrphanFiles), string(engine), taskInput)
	if err != nil {
		return 0, fmt.Errorf("could not enqueue remove orphan files task: %w", err)
	}

	return taskId, nil
}

func (s *ServiceTasks) EnqueueExpireSnapshotsBatch(ctx context.Context, tables []string, retentionDays int) (*BatchEnqueueResult, error) {
	return s.enqueueBatch(ctx, tables, func(cttx context.Context, table string) (int64, error) {
		return s.EnqueueExpireSnapshots(cttx, table, retentionDays)
	})
}

func (s *ServiceTasks) EnqueueRemoveOrphanFilesBatch(ctx context.Context, tables []string, retentionDays int) (*BatchEnqueueResult, error) {
	return s.enqueueBatch(ctx, tables, func(cttx context.Context, table string) (int64, error) {
		return s.EnqueueRemoveOrphanFiles(cttx, table, retentionDays)
	})
}

func (s *ServiceTasks) EnqueueOptimizeBatch(ctx context.Context, tables []BatchOptimizeTable, fileSizeThresholdMb int, from time.Time, to time.Time) (*BatchEnqueueResult, error) {
	if from.IsZero() || to.IsZero() {
		return nil, fmt.Errorf("from and to dates are required for optimize")
	}

	if from.After(to) {
		return nil, fmt.Errorf("from date must be before or equal to the to date")
	}

	normalizedTables := normalizeBatchOptimizeTables(tables)
	if len(normalizedTables) == 0 {
		return nil, fmt.Errorf("at least one table must be provided")
	}

	result := &BatchEnqueueResult{
		TaskIds:      make([]int64, 0, len(normalizedTables)),
		FailedTables: make([]BatchEnqueueFailure, 0),
	}

	for _, tableConfig := range normalizedTables {
		taskIDs, err := s.EnqueueOptimize(ctx, tableConfig.Table, fileSizeThresholdMb, from, to, tableConfig.ChunkBy)
		if err != nil {
			s.logger.Warn(ctx, "failed to enqueue optimize maintenance task for table %s: %s", tableConfig.Table, err)
			result.FailedTables = append(result.FailedTables, BatchEnqueueFailure{
				Table: tableConfig.Table,
				Error: err.Error(),
			})

			continue
		}

		result.TaskIds = append(result.TaskIds, taskIDs...)
		result.EnqueuedCount += int64(len(taskIDs))
	}

	return result, nil
}

// EnqueueOptimize queries the partitions table for partitions that need optimization
// within the given date range and enqueues one optimize task per qualifying chunk.
func (s *ServiceTasks) EnqueueOptimize(ctx context.Context, table string, fileSizeThresholdMb int, from time.Time, to time.Time, chunkBy string) ([]int64, error) {
	var err error
	var taskId int64
	var taskIds []int64
	chunkBy, err = normalizeOptimizeChunkBy(chunkBy)
	if err != nil {
		return nil, err
	}

	engine, err := s.engineResolver.Resolve(TaskKindOptimize)
	if err != nil {
		return nil, fmt.Errorf("could not resolve engine for optimize task: %w", err)
	}

	// Apply default threshold
	if fileSizeThresholdMb < 1 {
		fileSizeThresholdMb = 128
	}

	// Validate date range
	if from.IsZero() || to.IsZero() {
		return nil, fmt.Errorf("from and to dates are required for optimize")
	}

	if from.After(to) {
		return nil, fmt.Errorf("from date must be before or equal to the to date")
	}

	// Query partitions that need optimization within the date range
	// The partition column stores JSON like {"year": "2025", "month": "06", "day": "15"}
	// We need to construct a date from these fields and filter by the date range
	type partitionRow struct {
		Year  string `db:"year"`
		Month string `db:"month"`
		Day   string `db:"day"`
	}

	// Build a date path expression: CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0'))
	// Use sqlc.Concat() with LPAD to ensure zero-padded dates for proper string comparison
	datePath := sqlc.Concat(
		sqlc.Col("p.partition->>'$.year'"),
		sqlc.Literal("'-'"),
		sqlc.Col("p.partition->>'$.month'").Lpad(2, "0"),
		sqlc.Literal("'-'"),
		sqlc.Col("p.partition->>'$.day'").Lpad(2, "0"),
	)

	sel := s.sqlClient.Q().From("partitions").As("p").
		Column(sqlc.Col("p.partition->>'$.year'").As("year")).
		Column(sqlc.Col("p.partition->>'$.month'").As("month")).
		Column(sqlc.Col("p.partition->>'$.day'").As("day")).
		Where(sqlc.Eq{"p.table": table, "p.needs_optimize": true}).
		Where(datePath.Gte(from.Format(time.DateOnly))).
		Where(datePath.Lte(to.Format(time.DateOnly))).
		OrderBy(datePath.Asc())

	var partitions []partitionRow
	if err = sel.Select(ctx, &partitions); err != nil {
		return nil, fmt.Errorf("could not query partitions that need optimization: %w", err)
	}

	chunkSet := make([]optimizeRangeChunk, 0, len(partitions))
	seenChunks := make(map[string]struct{}, len(partitions))

	// Enqueue one task per chunk that contains at least one qualifying partition.
	for _, p := range partitions {
		dateStr := fmt.Sprintf("%s-%s-%s", p.Year, p.Month, p.Day)
		partitionDate, err := time.Parse("2006-1-2", dateStr)
		if err != nil {
			return nil, fmt.Errorf("could not parse partition date %s: %w", dateStr, err)
		}

		chunk := optimizeChunkForDate(partitionDate, chunkBy)
		if chunk.from.Before(from) {
			chunk.from = from
		}
		if chunk.to.After(to) {
			chunk.to = to
		}

		chunkKey := chunk.from.Format(time.DateOnly) + ":" + chunk.to.Format(time.DateOnly)
		if _, ok := seenChunks[chunkKey]; ok {
			continue
		}

		seenChunks[chunkKey] = struct{}{}
		chunkSet = append(chunkSet, chunk)
	}

	for _, chunk := range chunkSet {
		taskInput := map[string]any{
			"file_size_threshold_mb": fileSizeThresholdMb,
			"from":                   chunk.from,
			"to":                     chunk.to,
		}

		if taskId, err = s.serviceTaskQueue.EnqueueTask(ctx, table, string(TaskKindOptimize), string(engine), taskInput); err != nil {
			return nil, fmt.Errorf("could not enqueue optimize task for range %s to %s: %w", chunk.from.Format(time.DateOnly), chunk.to.Format(time.DateOnly), err)
		}
		taskIds = append(taskIds, taskId)
	}

	return taskIds, nil
}

func normalizeOptimizeChunkBy(chunkBy string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(chunkBy)) {
	case "", optimizeChunkDay:
		return optimizeChunkDay, nil
	case optimizeChunkWeek:
		return optimizeChunkWeek, nil
	case optimizeChunkMonth:
		return optimizeChunkMonth, nil
	default:
		return "", fmt.Errorf("unsupported optimize chunking %q", chunkBy)
	}
}

func optimizeChunkForDate(date time.Time, chunkBy string) optimizeRangeChunk {
	day := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)

	switch chunkBy {
	case optimizeChunkWeek:
		weekdayOffset := (int(day.Weekday()) + 6) % 7
		start := day.AddDate(0, 0, -weekdayOffset)

		return optimizeRangeChunk{
			from: start,
			to:   start.AddDate(0, 0, 6),
		}
	case optimizeChunkMonth:
		start := time.Date(day.Year(), day.Month(), 1, 0, 0, 0, 0, time.UTC)

		return optimizeRangeChunk{
			from: start,
			to:   start.AddDate(0, 1, -1),
		}
	default:
		return optimizeRangeChunk{
			from: day,
			to:   day,
		}
	}
}

func (s *ServiceTasks) enqueueBatch(ctx context.Context, tables []string, enqueue func(context.Context, string) (int64, error)) (*BatchEnqueueResult, error) {
	normalizedTables := normalizeBatchTables(tables)
	if len(normalizedTables) == 0 {
		return nil, fmt.Errorf("at least one table must be provided")
	}

	result := &BatchEnqueueResult{
		TaskIds:      make([]int64, 0, len(normalizedTables)),
		FailedTables: make([]BatchEnqueueFailure, 0),
	}

	for _, table := range normalizedTables {
		taskID, err := enqueue(ctx, table)
		if err != nil {
			s.logger.Warn(ctx, "failed to enqueue maintenance task for table %s: %s", table, err)
			result.FailedTables = append(result.FailedTables, BatchEnqueueFailure{
				Table: table,
				Error: err.Error(),
			})

			continue
		}

		result.TaskIds = append(result.TaskIds, taskID)
		result.EnqueuedCount++
	}

	return result, nil
}

func (s *ServiceTasks) RetryTask(ctx context.Context, taskID int64) (int64, error) {
	retryTaskID, err := s.serviceTaskQueue.RetryTask(ctx, taskID)
	if err != nil {
		return 0, fmt.Errorf("could not retry task %d: %w", taskID, err)
	}

	return retryTaskID, nil
}

func (s *ServiceTasks) RetryAllTasks(ctx context.Context) (int64, error) {
	retriedCount, err := s.serviceTaskQueue.RetryAllTasks(ctx)
	if err != nil {
		return 0, fmt.Errorf("could not retry failed tasks: %w", err)
	}

	return retriedCount, nil
}

func (s *ServiceTasks) UpdateProcedureResult(ctx context.Context, taskID int64, callback *TaskProcedureCallback) error {
	task, err := s.serviceTaskQueue.GetTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("could not load task %d for procedure callback: %w", taskID, err)
	}

	if TaskEngine(task.Engine) != TaskEngineSpark {
		return fmt.Errorf("task %d does not use spark engine", taskID)
	}

	if task.Status != taskStatusRunning {
		return fmt.Errorf("task %d cannot accept procedure callback in status %s", taskID, task.Status)
	}

	result := map[string]any{
		"query":       callback.Query,
		"rows":        callback.Rows,
		"received_at": callback.ReceivedAt,
	}

	if len(callback.Meta) > 0 {
		result["meta"] = callback.Meta
	}

	if err = s.serviceTaskQueue.UpdateTaskResultNested(ctx, taskID, taskProcedureResultKey, result); err != nil {
		return fmt.Errorf("could not update procedure result for task %d: %w", taskID, err)
	}

	return nil
}

func normalizeBatchTables(tables []string) []string {
	normalized := make([]string, 0, len(tables))
	seen := make(map[string]struct{}, len(tables))

	for _, table := range tables {
		trimmed := strings.TrimSpace(table)
		if trimmed == "" {
			continue
		}

		if _, ok := seen[trimmed]; ok {
			continue
		}

		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}

	return normalized
}

func normalizeBatchOptimizeTables(tables []BatchOptimizeTable) []BatchOptimizeTable {
	normalized := make([]BatchOptimizeTable, 0, len(tables))
	seen := make(map[string]struct{}, len(tables))

	for _, table := range tables {
		trimmedTable := strings.TrimSpace(table.Table)
		if trimmedTable == "" {
			continue
		}

		if _, ok := seen[trimmedTable]; ok {
			continue
		}

		seen[trimmedTable] = struct{}{}
		normalized = append(normalized, BatchOptimizeTable{
			Table:   trimmedTable,
			ChunkBy: strings.TrimSpace(table.ChunkBy),
		})
	}

	return normalized
}

// ListTasks is a pass-through to ServiceTaskQueue.ListTasks
func (s *ServiceTasks) ListTasks(ctx context.Context, table string, kinds []string, statuses []string, limit int, offset int) (*PaginatedTasks, error) {
	result, err := s.serviceTaskQueue.ListTasks(ctx, table, kinds, statuses, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("could not list tasks: %w", err)
	}

	return result, nil
}

// TaskCounts is a pass-through to ServiceTaskQueue.TaskCounts
func (s *ServiceTasks) TaskCounts(ctx context.Context) (running int64, queued int64, err error) {
	running, queued, err = s.serviceTaskQueue.TaskCounts(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("could not get task counts: %w", err)
	}

	return running, queued, nil
}

// FlushTasks is a pass-through to ServiceTaskQueue.FlushTasks
func (s *ServiceTasks) FlushTasks(ctx context.Context) (int64, error) {
	deleted, err := s.serviceTaskQueue.FlushTasks(ctx)
	if err != nil {
		return 0, fmt.Errorf("could not flush tasks: %w", err)
	}

	return deleted, nil
}
