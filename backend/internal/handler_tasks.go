package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/gosoline-project/httpserver"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type ExpireSnapshotsInput struct {
	Database      string `uri:"database"`
	Table         string `uri:"table"`
	RetentionDays int    `json:"retention_days"`
}

type RemoveOrphanFilesInput struct {
	Database      string `uri:"database"`
	Table         string `uri:"table"`
	RetentionDays int    `json:"retention_days"`
}

type OptimizeInput struct {
	Database         string   `uri:"database"`
	Table            string   `uri:"table"`
	TargetFileSizeMb int      `json:"target_file_size_mb"`
	From             DateTime `json:"from"`
	To               DateTime `json:"to"`
	ChunkBy          string   `json:"chunk_by"`
}

type ListTasksInput struct {
	Database string   `uri:"database"`
	Table    string   `form:"table"`
	Kind     []string `form:"kind"`
	Status   []string `form:"status"`
	Limit    int      `form:"limit"`
	Offset   int      `form:"offset"`
}

type RetryTaskInput struct {
	Id int64 `uri:"id"`
}

type TaskProcedureCallbackInput struct {
	Id    int64            `uri:"id"`
	Query string           `json:"query"`
	Rows  []map[string]any `json:"rows"`
	Meta  map[string]any   `json:"meta"`
}

type TaskQueuedResponse struct {
	TaskId int64  `json:"task_id"`
	Status string `json:"status"`
}

type OptimizeTaskQueuedResponse struct {
	TaskIds []int64 `json:"task_ids"`
	Status  string  `json:"status"`
}

type TaskCountsResponse struct {
	Running int64 `json:"running"`
	Queued  int64 `json:"queued"`
}

type FlushTasksResponse struct {
	Deleted int64 `json:"deleted"`
}

type RetryAllTasksResponse struct {
	RetriedCount int64 `json:"retried_count"`
}

func NewHandlerTasks(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerTasks, error) {
	var err error
	var serviceTasks *ServiceTasks

	if serviceTasks, err = NewServiceTasks(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create tasks service: %w", err)
	}

	return &HandlerTasks{
		serviceTasks: serviceTasks,
	}, nil
}

type HandlerTasks struct {
	serviceTasks *ServiceTasks
}

func (h *HandlerTasks) ExpireSnapshots(ctx context.Context, input *ExpireSnapshotsInput) (httpserver.Response, error) {
	taskId, err := h.serviceTasks.EnqueueExpireSnapshots(ctx, input.Database, input.Table, input.RetentionDays)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(&TaskQueuedResponse{
		TaskId: taskId,
		Status: taskStatusQueued,
	}), nil
}

func (h *HandlerTasks) RemoveOrphanFiles(ctx context.Context, input *RemoveOrphanFilesInput) (httpserver.Response, error) {
	taskId, err := h.serviceTasks.EnqueueRemoveOrphanFiles(ctx, input.Database, input.Table, input.RetentionDays)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(&TaskQueuedResponse{
		TaskId: taskId,
		Status: taskStatusQueued,
	}), nil
}

func (h *HandlerTasks) Optimize(ctx context.Context, input *OptimizeInput) (httpserver.Response, error) {
	taskIds, err := h.serviceTasks.EnqueueOptimize(ctx, input.Database, input.Table, input.TargetFileSizeMb, input.From.Time, input.To.Time, input.ChunkBy)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(&OptimizeTaskQueuedResponse{
		TaskIds: taskIds,
		Status:  taskStatusQueued,
	}), nil
}

func (h *HandlerTasks) ListTasks(ctx context.Context, input *ListTasksInput) (httpserver.Response, error) {
	result, err := h.serviceTasks.ListTasks(ctx, input.Database, input.Table, input.Kind, input.Status, input.Limit, input.Offset)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(result), nil
}

func (h *HandlerTasks) RetryTask(ctx context.Context, input *RetryTaskInput) (httpserver.Response, error) {
	taskId, err := h.serviceTasks.RetryTask(ctx, input.Id)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(&TaskQueuedResponse{
		TaskId: taskId,
		Status: taskStatusQueued,
	}), nil
}

func (h *HandlerTasks) RetryAllTasks(ctx context.Context) (httpserver.Response, error) {
	retriedCount, err := h.serviceTasks.RetryAllTasks(ctx)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(&RetryAllTasksResponse{
		RetriedCount: retriedCount,
	}), nil
}

func (h *HandlerTasks) ProcedureResultCallback(ctx context.Context, input *TaskProcedureCallbackInput) (httpserver.Response, error) {
	callback := &TaskProcedureCallback{
		Query:      input.Query,
		Rows:       input.Rows,
		Meta:       input.Meta,
		ReceivedAt: DateTime{Time: time.Now().UTC()},
	}

	if err := h.serviceTasks.UpdateProcedureResult(ctx, input.Id, callback); err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(map[string]string{"status": statusOK}), nil
}

func (h *HandlerTasks) TaskCounts(ctx context.Context, input *ListTablesInput) (httpserver.Response, error) {
	running, queued, err := h.serviceTasks.TaskCounts(ctx, input.Database)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(&TaskCountsResponse{
		Running: running,
		Queued:  queued,
	}), nil
}

func (h *HandlerTasks) FlushTasks(ctx context.Context, input *ListTablesInput) (httpserver.Response, error) {
	deleted, err := h.serviceTasks.FlushTasks(ctx, input.Database)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(&FlushTasksResponse{
		Deleted: deleted,
	}), nil
}
