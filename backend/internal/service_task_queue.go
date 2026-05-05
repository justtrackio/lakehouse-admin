package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/db"
	"github.com/justtrackio/gosoline/pkg/log"
)

type ServiceTaskQueue struct {
	logger                 log.Logger
	sqlClient              sqlc.Client
	serviceSettings        *ServiceSettings
	defaultTaskConcurrency int
}

var errTaskCompletionNotFound = errors.New("task not found for completion")

func NewServiceTaskQueue(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceTaskQueue, error) {
	var err error
	var sqlClient sqlc.Client
	var serviceSettings *ServiceSettings
	var defaultTaskConcurrency int

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}

	if serviceSettings, err = NewServiceSettings(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create settings service: %w", err)
	}

	if defaultTaskConcurrency, err = config.GetInt("tasks.worker_count"); err != nil || defaultTaskConcurrency < 1 {
		defaultTaskConcurrency = 1
	}

	return &ServiceTaskQueue{
		logger:                 logger.WithChannel("task_queue"),
		sqlClient:              sqlClient,
		serviceSettings:        serviceSettings,
		defaultTaskConcurrency: defaultTaskConcurrency,
	}, nil
}

func (s *ServiceTaskQueue) EnqueueTask(ctx context.Context, database string, table string, kind string, engine string, input map[string]any) (int64, error) {
	var err error
	var res sqlc.Result
	var id int64

	entry := newQueuedTask(database, table, kind, engine, input)

	ins := s.sqlClient.Q().Into("tasks").Records(entry)
	if res, err = ins.Exec(ctx); err != nil {
		return 0, fmt.Errorf("could not enqueue task: %w", err)
	}

	if id, err = res.LastInsertId(); err != nil {
		return 0, fmt.Errorf("could not get last insert id: %w", err)
	}

	return id, nil
}

func (s *ServiceTaskQueue) GetTask(ctx context.Context, id int64) (*Task, error) {
	var task Task

	stmt := s.sqlClient.Q().From("tasks").Where(sqlc.Eq{"id": id}).Limit(1)
	if err := stmt.Get(ctx, &task); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("task %d not found", id)
		}

		return nil, fmt.Errorf("could not load task %d: %w", id, err)
	}

	return &task, nil
}

func (s *ServiceTaskQueue) RetryTask(ctx context.Context, id int64) (int64, error) {
	var retryTaskID int64

	err := s.sqlClient.WithTx(ctx, func(cttx sqlc.Tx) error {
		task, err := s.getTaskForRetry(cttx, id)
		if err != nil {
			return err
		}

		retryTaskID, err = s.retryTaskInTx(cttx, task)
		if err != nil {
			return err
		}

		return nil
	}, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, err
	}

	return retryTaskID, nil
}

func (s *ServiceTaskQueue) RetryAllTasks(ctx context.Context, database string) (int64, error) {
	var retriedCount int64

	err := s.sqlClient.WithTx(ctx, func(cttx sqlc.Tx) error {
		var tasks []Task

		query := cttx.Q().
			From("tasks").
			Where(sqlc.Eq{"status": taskStatusError, "retried": false}).
			OrderBy(sqlc.Col("started_at").Asc())

		if database != "" {
			query = query.Where(sqlc.Eq{"database": database})
		}

		if err := query.Select(cttx, &tasks); err != nil {
			return fmt.Errorf("could not list retryable tasks: %w", err)
		}

		for i := range tasks {
			if _, err := s.retryTaskInTx(cttx, &tasks[i]); err != nil {
				if errors.Is(err, errTaskAlreadyRetried) {
					continue
				}

				return err
			}

			retriedCount++
		}

		return nil
	}, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, err
	}

	return retriedCount, nil
}

var errTaskAlreadyRetried = errors.New("task already retried")

func (s *ServiceTaskQueue) getTaskForRetry(ctx sqlc.Tx, id int64) (*Task, error) {
	var task Task

	stmt := ctx.Q().From("tasks").Where(sqlc.Eq{"id": id}).Limit(1)
	if err := stmt.Get(ctx, &task); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("task %d not found", id)
		}

		return nil, fmt.Errorf("could not load task %d: %w", id, err)
	}

	return &task, nil
}

func (s *ServiceTaskQueue) retryTaskInTx(ctx sqlc.Tx, task *Task) (int64, error) {
	if task.Status != taskStatusError {
		return 0, fmt.Errorf("task %d cannot be retried because it is in status %s", task.Id, task.Status)
	}

	if task.Retried {
		return 0, fmt.Errorf("task %d has already been retried: %w", task.Id, errTaskAlreadyRetried)
	}

	update := ctx.Q().Update("tasks").Set("retried", true).Where(sqlc.Eq{"id": task.Id, "status": taskStatusError, "retried": false})
	res, err := update.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("could not mark task %d as retried: %w", task.Id, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("could not get rows affected when retrying task %d: %w", task.Id, err)
	}

	if affected == 0 {
		return 0, fmt.Errorf("task %d has already been retried: %w", task.Id, errTaskAlreadyRetried)
	}

	insert := ctx.Q().Into("tasks").Records(newQueuedTask(task.Database, task.Table, task.Kind, task.Engine, task.Input.Get()))
	res, err = insert.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("could not enqueue retry for task %d: %w", task.Id, err)
	}

	retryTaskID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("could not get retry task id for task %d: %w", task.Id, err)
	}

	return retryTaskID, nil
}

func newQueuedTask(database string, table string, kind string, engine string, input map[string]any) *Task {
	if input == nil {
		input = map[string]any{}
	}

	return &Task{
		Database:  database,
		Table:     table,
		Kind:      kind,
		Engine:    engine,
		StartedAt: time.Now(),
		Status:    taskStatusQueued,
		Retried:   false,
		Input:     db.NewJSON(input, db.NonNullable{}),
		Result:    db.NewJSON(map[string]any{}, db.NonNullable{}),
	}
}

func (s *ServiceTaskQueue) ClaimTask(ctx context.Context) (*Task, error) {
	taskConcurrency, err := s.serviceSettings.GetIntSetting(ctx, "task_concurrency", s.defaultTaskConcurrency)
	if err != nil {
		return nil, fmt.Errorf("could not load task concurrency setting: %w", err)
	}

	if taskConcurrency < 1 {
		taskConcurrency = 1
	}

	var claimedTask *Task

	for i := 0; i < 3; i++ {
		err = s.sqlClient.WithTx(ctx, func(cttx sqlc.Tx) error {
			return s.claimTaskWithConcurrency(cttx, taskConcurrency, &claimedTask)
		}, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err == nil {
			return claimedTask, nil
		}

		if !isTaskClaimRetryable(err) {
			return nil, err
		}
	}

	return nil, fmt.Errorf("could not claim task after retries: %w", err)
}

func (s *ServiceTaskQueue) claimTaskWithConcurrency(ctx sqlc.Tx, taskConcurrency int, claimedTask **Task) error {
	var err error
	var res sqlc.Result
	var affected int64

	var runningCount struct {
		Count int `db:"count"`
	}

	stmt := ctx.Q().From("tasks").Column(sqlc.Col("*").Count().As("count")).Where(sqlc.Eq{"status": taskStatusRunning})
	if err := stmt.Get(ctx, &runningCount); err != nil {
		return fmt.Errorf("could not count running tasks: %w", err)
	}

	if runningCount.Count >= taskConcurrency {
		*claimedTask = nil

		return nil
	}

	var task Task
	stmt = ctx.Q().From("tasks").Where(sqlc.Eq{"status": taskStatusQueued}).OrderBy(sqlc.Col("started_at").Asc()).Limit(1)
	if err := stmt.Get(ctx, &task); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			*claimedTask = nil

			return nil
		}

		return fmt.Errorf("could not select queued task: %w", err)
	}

	now := time.Now()
	upd := ctx.Q().Update("tasks").Set("status", taskStatusRunning).Set("picked_up_at", &now).Where(sqlc.Eq{"id": task.Id, "status": taskStatusQueued})
	if res, err = upd.Exec(ctx); err != nil {
		return fmt.Errorf("could not update task status to running: %w", err)
	}

	if affected, err = res.RowsAffected(); err != nil {
		return fmt.Errorf("could not get rows affected: %w", err)
	}

	if affected == 0 {
		return fmt.Errorf("queued task %d could not be claimed due to concurrent update", task.Id)
	}

	task.Status = taskStatusRunning
	task.PickedUpAt = &now
	*claimedTask = &task

	return nil
}

func isTaskClaimRetryable(err error) bool {
	if err == nil {
		return false
	}

	errMessage := err.Error()

	errMessage = strings.ToLower(errMessage)

	return strings.Contains(errMessage, "deadlock") || strings.Contains(errMessage, "concurrent update")
}

func (s *ServiceTaskQueue) CompleteTask(ctx context.Context, id int64, result map[string]any, err error) error {
	status := taskStatusSuccess
	var errMsg *string

	if err != nil {
		status = taskStatusError
		msg := err.Error()
		errMsg = &msg
	}

	var task Task
	if getErr := s.sqlClient.Q().From("tasks").Where(sqlc.Eq{"id": id}).Limit(1).Get(ctx, &task); getErr != nil {
		if errors.Is(getErr, sql.ErrNoRows) {
			return fmt.Errorf("task %d not found for completion: %w", id, errTaskCompletionNotFound)
		}

		return fmt.Errorf("could not load task for completion: %w", getErr)
	}

	mergedResult := mergeTaskResult(task.Result.Get(), result)

	now := time.Now()
	upd := s.sqlClient.Q().Update("tasks").
		Set("finished_at", &now).
		Set("status", status).
		Set("error_message", errMsg).
		Set("result", db.NewJSON(mergedResult, db.NonNullable{})).
		Where(sqlc.Eq{"id": id, "status": taskStatusRunning})

	res, err := upd.Exec(ctx)
	if err != nil {
		return fmt.Errorf("could not complete task: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("could not get rows affected when completing task: %w", err)
	}

	if affected == 0 {
		return nil
	}

	return nil
}

func (s *ServiceTaskQueue) UpdateTaskResult(ctx context.Context, id int64, result map[string]any) error {
	var task Task
	if err := s.sqlClient.Q().From("tasks").Where(sqlc.Eq{"id": id}).Limit(1).Get(ctx, &task); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("task %d not found for result update", id)
		}

		return fmt.Errorf("could not load task for result update: %w", err)
	}

	mergedResult := mergeTaskResult(task.Result.Get(), result)

	upd := s.sqlClient.Q().Update("tasks").
		Set("result", db.NewJSON(mergedResult, db.NonNullable{})).
		Where(sqlc.Eq{"id": id})

	if _, err := upd.Exec(ctx); err != nil {
		return fmt.Errorf("could not update task result: %w", err)
	}

	return nil
}

func (s *ServiceTaskQueue) UpdateTaskResultNested(ctx context.Context, id int64, key string, result map[string]any) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("result key is required")
	}

	if result == nil {
		result = map[string]any{}
	}

	return s.UpdateTaskResult(ctx, id, map[string]any{key: result})
}

func mergeTaskResult(existing map[string]any, update map[string]any) map[string]any {
	merged := make(map[string]any)

	for key, value := range existing {
		merged[key] = value
	}

	for key, value := range update {
		merged[key] = value
	}

	return merged
}

func (s *ServiceTaskQueue) TaskCounts(ctx context.Context, database string) (running int64, queued int64, err error) {
	var results []struct {
		Status string `db:"status"`
		Count  int64  `db:"count"`
	}

	// Get counts grouped by status for running and queued tasks only
	query := s.sqlClient.Q().
		From("tasks").
		Column(sqlc.Col("status")).
		Column(sqlc.Col("*").Count().As("count")).
		Where(sqlc.Col("status").In(taskStatusQueued, taskStatusRunning)).
		GroupBy(sqlc.Col("status"))

	if database != "" {
		query = query.Where(sqlc.Eq{"database": database})
	}

	if err = query.Select(ctx, &results); err != nil {
		return 0, 0, fmt.Errorf("could not get task counts: %w", err)
	}

	for _, r := range results {
		switch r.Status {
		case taskStatusRunning:
			running = r.Count
		case taskStatusQueued:
			queued = r.Count
		}
	}

	return running, queued, nil
}

func (s *ServiceTaskQueue) ListTasks(ctx context.Context, database string, table string, kinds []string, statuses []string, limit int, offset int) (*PaginatedTasks, error) {
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
	if database != "" {
		cnt = cnt.Where(sqlc.Eq{"database": database})
	}
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
	if database != "" {
		sel = sel.Where(sqlc.Eq{"database": database})
	}

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
			Database:     r.Database,
			Table:        r.Table,
			Kind:         r.Kind,
			Engine:       r.Engine,
			StartedAt:    r.StartedAt,
			PickedUpAt:   r.PickedUpAt,
			FinishedAt:   r.FinishedAt,
			Status:       r.Status,
			Retried:      r.Retried,
			CanRetry:     r.Status == taskStatusError && !r.Retried,
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

func (s *ServiceTaskQueue) FlushTasks(ctx context.Context, database string) (int64, error) {
	var err error
	var res sqlc.Result
	var affected int64

	del := s.sqlClient.Q().Delete("tasks")
	if database != "" {
		del = del.Where(sqlc.Eq{"database": database})
	}
	if res, err = del.Exec(ctx); err != nil {
		return 0, fmt.Errorf("could not flush tasks: %w", err)
	}

	if affected, err = res.RowsAffected(); err != nil {
		return 0, fmt.Errorf("could not get rows affected: %w", err)
	}

	return affected, nil
}
