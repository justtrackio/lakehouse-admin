package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/db"
	"github.com/justtrackio/gosoline/pkg/log"
)

const minRetentionDays = 7
const minRetainLast = 10

type ServiceMaintenance struct {
	logger    log.Logger
	sqlClient sqlc.Client
}

func NewServiceMaintenance(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceMaintenance, error) {
	var err error
	var sqlClient sqlc.Client

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}

	return &ServiceMaintenance{
		logger:    logger.WithChannel("maintenance_queue"),
		sqlClient: sqlClient,
	}, nil
}

func (s *ServiceMaintenance) QueueExpireSnapshots(ctx context.Context, table string, retentionDays int, retainLast int) (int64, error) {
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

	return s.EnqueueTask(ctx, table, "expire_snapshots", taskInput)
}

func (s *ServiceMaintenance) QueueRemoveOrphanFiles(ctx context.Context, table string, retentionDays int) (int64, error) {
	if retentionDays < minRetentionDays {
		retentionDays = minRetentionDays
	}

	taskInput := map[string]any{
		"retention_days": retentionDays,
	}

	return s.EnqueueTask(ctx, table, "remove_orphan_files", taskInput)
}

func (s *ServiceMaintenance) QueueOptimize(ctx context.Context, table string, fileSizeThresholdMb int, from time.Time, to time.Time) ([]int64, error) {
	var err error
	var taskId int64
	var taskIds []int64

	if fileSizeThresholdMb < 1 {
		fileSizeThresholdMb = 128
	}

	if from.IsZero() || to.IsZero() {
		return nil, fmt.Errorf("from and to dates are required for optimize")
	}

	if from.After(to) {
		return nil, fmt.Errorf("from date must be before or equal to to date")
	}

	// Calculate chunks
	type chunk struct {
		from time.Time
		to   time.Time
	}
	var chunks []chunk

	current := from
	for !current.After(to) {
		var chunkEnd time.Time

		// Daily batch size
		chunkEnd = current // Start == End for daily

		// Clamp to 'to'
		if chunkEnd.After(to) {
			chunkEnd = to
		}

		chunks = append(chunks, chunk{from: current, to: chunkEnd})

		// Next start is day after chunkEnd
		current = chunkEnd.AddDate(0, 0, 1)
	}

	for _, c := range chunks {
		taskInput := map[string]any{
			"file_size_threshold_mb": fileSizeThresholdMb,
			"from":                   DateTime{Time: c.from},
			"to":                     DateTime{Time: c.to},
		}

		if taskId, err = s.EnqueueTask(ctx, table, "optimize", taskInput); err != nil {
			return nil, fmt.Errorf("could not enqueue optimize task: %w", err)
		}
		taskIds = append(taskIds, taskId)
	}

	return taskIds, nil
}

func (s *ServiceMaintenance) EnqueueTask(ctx context.Context, table string, kind string, input map[string]any) (int64, error) {
	entry := &MaintenanceTask{
		Table:     table,
		Kind:      kind,
		StartedAt: time.Now(),
		Status:    "queued",
		Input:     db.NewJSON(input, db.NonNullable{}),
		Result:    db.NewJSON(map[string]any{}, db.NonNullable{}),
	}

	ins := s.sqlClient.Q().Into("maintenance_tasks").Records(entry)
	res, err := ins.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("could not enqueue maintenance task: %w", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("could not get last insert id: %w", err)
	}

	return id, nil
}

func (s *ServiceMaintenance) ClaimTask(ctx context.Context) (*MaintenanceTask, error) {
	// Optimistic locking loop
	for i := 0; i < 3; i++ {
		var task MaintenanceTask
		// 1. Find oldest queued task
		// Note: We avoid FOR UPDATE SKIP LOCKED as it might not be supported by the query builder directly
		err := s.sqlClient.Q().From("maintenance_tasks").
			Where(sqlc.Eq{"status": "queued"}).
			OrderBy(sqlc.Col("started_at").Asc()).
			Limit(1).
			Get(ctx, &task)

		if err != nil {
			// If we can't find a task, we assume the queue is empty.
			// In a more robust implementation, we would check for specific "not found" error.
			return nil, nil
		}

		// 2. Try to claim it atomically
		now := time.Now()
		upd := s.sqlClient.Q().Update("maintenance_tasks").
			Set("status", "running").
			Set("picked_up_at", &now).
			Where(sqlc.Eq{"id": task.Id, "status": "queued"})

		res, err := upd.Exec(ctx)
		if err != nil {
			return nil, fmt.Errorf("could not update task status to running: %w", err)
		}

		affected, err := res.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("could not get rows affected: %w", err)
		}

		if affected > 0 {
			task.Status = "running"
			task.PickedUpAt = &now
			return &task, nil
		}
		// If affected == 0, another worker claimed it between Step 1 and 2. Retry.
	}

	return nil, nil
}

func (s *ServiceMaintenance) CompleteTask(ctx context.Context, id int64, result map[string]any, err error) error {
	status := "success"
	var errMsg *string

	if err != nil {
		status = "error"
		msg := err.Error()
		errMsg = &msg
	}

	now := time.Now()
	upd := s.sqlClient.Q().Update("maintenance_tasks").
		Set("finished_at", &now).
		Set("status", status).
		Set("error_message", errMsg).
		Set("result", db.NewJSON(result, db.NonNullable{})).
		Where(sqlc.Eq{"id": id})

	if _, err := upd.Exec(ctx); err != nil {
		return fmt.Errorf("could not complete task: %w", err)
	}

	return nil
}

func (s *ServiceMaintenance) ListTasks(ctx context.Context, table string, kinds []string, statuses []string, limit int, offset int) (*PaginatedMaintenanceTask, error) {
	var err error
	var result []MaintenanceTask
	var count struct {
		Total int64 `db:"total"`
	}

	kindsAny := make([]any, len(kinds))
	for i, k := range kinds {
		kindsAny[i] = k
	}
	statusesAny := make([]any, len(statuses))
	for i, st := range statuses {
		statusesAny[i] = st
	}


	if limit <= 0 {
		limit = 20
	}

	if offset < 0 {
		offset = 0
	}

	// 1. Get total count
	cnt := s.sqlClient.Q().From("maintenance_tasks").Column(sqlc.Col("*").Count().As("total"))
	if table != "" {
		cnt = cnt.Where(sqlc.Eq{"table": table})
	}
	if len(kindsAny) > 0 {
		cnt = cnt.Where(sqlc.Col("kind").In(kindsAny...))
	}
	if len(statusesAny) > 0 {
		cnt = cnt.Where(sqlc.Col("status").In(statusesAny...))
	}

	if err = cnt.Get(ctx, &count); err != nil {
		return nil, fmt.Errorf("could not get maintenance task count: %w", err)
	}

	// 2. Get paginated items
	sel := s.sqlClient.Q().From("maintenance_tasks").OrderBy(sqlc.Col("started_at").Desc())

	if table != "" {
		sel = sel.Where(sqlc.Eq{"table": table})
	}
	if len(kindsAny) > 0 {
		sel = sel.Where(sqlc.Col("kind").In(kindsAny...))
	}
	if len(statusesAny) > 0 {
		sel = sel.Where(sqlc.Col("status").In(statusesAny...))
	}

	sel = sel.Limit(limit).Offset(offset)

	if err = sel.Select(ctx, &result); err != nil {
		return nil, fmt.Errorf("could not list maintenance tasks: %w", err)
	}

	// Convert to DTO
	dtos := make([]sMaintenanceTask, len(result))
	for i, r := range result {
		dtos[i] = sMaintenanceTask{
			Id:           r.Id,
			Table:        r.Table,
			Kind:         r.Kind,
			StartedAt:    r.StartedAt,
			PickedUpAt:   r.PickedUpAt,
			FinishedAt:   r.FinishedAt,
			Status:       r.Status,
			ErrorMessage: r.ErrorMessage,
			Input:        r.Input.Get(),
			Result:       r.Result.Get(),
		}
	}

	return &PaginatedMaintenanceTask{
		Items: dtos,
		Total: count.Total,
	}, nil
}
