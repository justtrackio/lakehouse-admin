package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

const (
	minRetentionDays = 7
	minRetainLast    = 10
)

type ServiceTasks struct {
	logger           log.Logger
	serviceTaskQueue *ServiceTaskQueue
	sqlClient        sqlc.Client
}

func NewServiceTasks(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceTasks, error) {
	var err error
	var serviceTaskQueue *ServiceTaskQueue
	var sqlClient sqlc.Client

	if serviceTaskQueue, err = NewServiceTaskQueue(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create task queue service: %w", err)
	}

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sql client: %w", err)
	}

	return &ServiceTasks{
		logger:           logger.WithChannel("tasks"),
		serviceTaskQueue: serviceTaskQueue,
		sqlClient:        sqlClient,
	}, nil
}

// EnqueueExpireSnapshots enqueues a task to expire old snapshots for a table
func (s *ServiceTasks) EnqueueExpireSnapshots(ctx context.Context, table string, retentionDays int, retainLast int) (int64, error) {
	// Apply minimum constraints
	if retentionDays < minRetentionDays {
		retentionDays = minRetentionDays
	}

	if retainLast < minRetainLast {
		retainLast = minRetainLast
	}

	taskInput := map[string]any{
		"retention_days": retentionDays,
		"retain_last":    retainLast,
	}

	taskId, err := s.serviceTaskQueue.EnqueueTask(ctx, table, "expire_snapshots", taskInput)
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

	taskId, err := s.serviceTaskQueue.EnqueueTask(ctx, table, "remove_orphan_files", taskInput)
	if err != nil {
		return 0, fmt.Errorf("could not enqueue remove orphan files task: %w", err)
	}

	return taskId, nil
}

// EnqueueOptimize queries the partitions table for partitions that need optimization
// within the given date range and enqueues one optimize task per qualifying partition
func (s *ServiceTasks) EnqueueOptimize(ctx context.Context, table string, fileSizeThresholdMb int, from time.Time, to time.Time) ([]int64, error) {
	var err error
	var taskId int64
	var taskIds []int64

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

	// Enqueue one task per partition that needs optimization
	for _, p := range partitions {
		// Construct the date for this partition
		// Use flexible date parsing format to handle unpadded month/day values
		dateStr := fmt.Sprintf("%s-%s-%s", p.Year, p.Month, p.Day)
		partitionDate, err := time.Parse("2006-1-2", dateStr)
		if err != nil {
			return nil, fmt.Errorf("could not parse partition date %s: %w", dateStr, err)
		}

		taskInput := map[string]any{
			"file_size_threshold_mb": fileSizeThresholdMb,
			"from":                   partitionDate,
			"to":                     partitionDate, // Single day
		}

		if taskId, err = s.serviceTaskQueue.EnqueueTask(ctx, table, "optimize", taskInput); err != nil {
			return nil, fmt.Errorf("could not enqueue optimize task for date %s: %w", dateStr, err)
		}
		taskIds = append(taskIds, taskId)
	}

	return taskIds, nil
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
