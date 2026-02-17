package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/db"
	"github.com/justtrackio/gosoline/pkg/log"
)

type ServiceTaskQueue struct {
	logger    log.Logger
	sqlClient sqlc.Client
}

func NewServiceTaskQueue(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceTaskQueue, error) {
	var err error
	var sqlClient sqlc.Client

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}

	return &ServiceTaskQueue{
		logger:    logger.WithChannel("task_queue"),
		sqlClient: sqlClient,
	}, nil
}

func (s *ServiceTaskQueue) EnqueueTask(ctx context.Context, table string, kind string, input map[string]any) (int64, error) {
	var err error
	var res sqlc.Result
	var id int64

	entry := &Task{
		Table:     table,
		Kind:      kind,
		StartedAt: time.Now(),
		Status:    "queued",
		Input:     db.NewJSON(input, db.NonNullable{}),
		Result:    db.NewJSON(map[string]any{}, db.NonNullable{}),
	}

	ins := s.sqlClient.Q().Into("tasks").Records(entry)
	if res, err = ins.Exec(ctx); err != nil {
		return 0, fmt.Errorf("could not enqueue task: %w", err)
	}

	if id, err = res.LastInsertId(); err != nil {
		return 0, fmt.Errorf("could not get last insert id: %w", err)
	}

	return id, nil
}

func (s *ServiceTaskQueue) ClaimTask(ctx context.Context) (*Task, error) {
	var err error
	var res sqlc.Result
	var affected int64

	// Optimistic locking loop
	for i := 0; i < 3; i++ {
		var task Task
		// 1. Find oldest queued task that doesn't have another task running for the same table
		// Use raw SQL for the NOT IN subquery since sqlc's NotIn() only supports scalar values
		err = s.sqlClient.Q().From("tasks").
			Where(sqlc.Eq{"status": "queued"}).
			Where("`table` NOT IN (SELECT `table` FROM `tasks` WHERE `status` = ?)", "running").
			OrderBy(sqlc.Col("started_at").Asc()).
			Limit(1).
			Get(ctx, &task)
		if err != nil {
			// If we can't find a task, we assume the queue is empty.
			return nil, nil
		}

		// 2. Try to claim it atomically
		now := time.Now()
		upd := s.sqlClient.Q().Update("tasks").
			Set("status", "running").
			Set("picked_up_at", &now).
			Where(sqlc.Eq{"id": task.Id, "status": "queued"})

		if res, err = upd.Exec(ctx); err != nil {
			return nil, fmt.Errorf("could not update task status to running: %w", err)
		}

		if affected, err = res.RowsAffected(); err != nil {
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

func (s *ServiceTaskQueue) CompleteTask(ctx context.Context, id int64, result map[string]any, err error) error {
	status := "success"
	var errMsg *string

	if err != nil {
		status = "error"
		msg := err.Error()
		errMsg = &msg
	}

	now := time.Now()
	upd := s.sqlClient.Q().Update("tasks").
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

func (s *ServiceTaskQueue) TaskCounts(ctx context.Context) (running int64, queued int64, err error) {
	var results []struct {
		Status string `db:"status"`
		Count  int64  `db:"count"`
	}

	// Get counts grouped by status for running and queued tasks only
	query := s.sqlClient.Q().
		From("tasks").
		Column(sqlc.Col("status")).
		Column(sqlc.Col("*").Count().As("count")).
		Where(sqlc.Col("status").In("queued", "running")).
		GroupBy(sqlc.Col("status"))

	if err = query.Select(ctx, &results); err != nil {
		return 0, 0, fmt.Errorf("could not get task counts: %w", err)
	}

	for _, r := range results {
		switch r.Status {
		case "running":
			running = r.Count
		case "queued":
			queued = r.Count
		}
	}

	return running, queued, nil
}

func (s *ServiceTaskQueue) ListTasks(ctx context.Context, table string, kinds []string, statuses []string, limit int, offset int) (*PaginatedTasks, error) {
	var err error
	var result []Task
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
	cnt := s.sqlClient.Q().From("tasks").Column(sqlc.Col("*").Count().As("total"))
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
		return nil, fmt.Errorf("could not get task count: %w", err)
	}

	// 2. Get paginated items
	sel := s.sqlClient.Q().From("tasks").OrderBy(sqlc.Col("started_at").Desc())

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
		return nil, fmt.Errorf("could not list tasks: %w", err)
	}

	// Convert to DTO
	dtos := make([]sTask, len(result))
	for i, r := range result {
		dtos[i] = sTask{
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

	return &PaginatedTasks{
		Items: dtos,
		Total: count.Total,
	}, nil
}

func (s *ServiceTaskQueue) FlushTasks(ctx context.Context) (int64, error) {
	var err error
	var res sqlc.Result
	var affected int64

	del := s.sqlClient.Q().Delete("tasks")
	if res, err = del.Exec(ctx); err != nil {
		return 0, fmt.Errorf("could not flush tasks: %w", err)
	}

	if affected, err = res.RowsAffected(); err != nil {
		return 0, fmt.Errorf("could not get rows affected: %w", err)
	}

	return affected, nil
}
