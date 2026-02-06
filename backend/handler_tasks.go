package main

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type ExpireSnapshotsInput struct {
	Table         string `uri:"table"`
	RetentionDays int    `json:"retention_days"`
	RetainLast    int    `json:"retain_last"`
}

type RemoveOrphanFilesInput struct {
	Table         string `uri:"table"`
	RetentionDays int    `json:"retention_days"`
}

type OptimizeInput struct {
	Table               string   `uri:"table"`
	FileSizeThresholdMb int      `json:"file_size_threshold_mb"`
	From                DateTime `json:"from"`
	To                  DateTime `json:"to"`
}

type ListTasksInput struct {
	Table  string   `form:"table"`
	Kind   []string `form:"kind"`
	Status []string `form:"status"`
	Limit  int      `form:"limit"`
	Offset int      `form:"offset"`
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
	taskId, err := h.serviceTasks.EnqueueExpireSnapshots(ctx, input.Table, input.RetentionDays, input.RetainLast)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(&TaskQueuedResponse{
		TaskId: taskId,
		Status: "queued",
	}), nil
}

func (h *HandlerTasks) RemoveOrphanFiles(ctx context.Context, input *RemoveOrphanFilesInput) (httpserver.Response, error) {
	taskId, err := h.serviceTasks.EnqueueRemoveOrphanFiles(ctx, input.Table, input.RetentionDays)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(&TaskQueuedResponse{
		TaskId: taskId,
		Status: "queued",
	}), nil
}

func (h *HandlerTasks) Optimize(ctx context.Context, input *OptimizeInput) (httpserver.Response, error) {
	taskIds, err := h.serviceTasks.EnqueueOptimize(ctx, input.Table, input.FileSizeThresholdMb, input.From.Time, input.To.Time)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(&OptimizeTaskQueuedResponse{
		TaskIds: taskIds,
		Status:  "queued",
	}), nil
}

func (h *HandlerTasks) ListTasks(ctx context.Context, input *ListTasksInput) (httpserver.Response, error) {
	result, err := h.serviceTasks.ListTasks(ctx, input.Table, input.Kind, input.Status, input.Limit, input.Offset)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(result), nil
}

func (h *HandlerTasks) TaskCounts(ctx context.Context) (httpserver.Response, error) {
	running, queued, err := h.serviceTasks.TaskCounts(ctx)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(&TaskCountsResponse{
		Running: running,
		Queued:  queued,
	}), nil
}
