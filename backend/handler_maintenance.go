package main

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/gosoline-project/sqlc"
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

func NewHandlerMaintenance(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerMaintenance, error) {
	var err error
	var serviceMaintenance *ServiceMaintenance

	if serviceMaintenance, err = NewServiceMaintenance(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create maintenance task queue service: %w", err)
	}

	return &HandlerMaintenance{
		serviceMaintenance: serviceMaintenance,
	}, nil
}

type HandlerMaintenance struct {
	serviceMaintenance *ServiceMaintenance
}

func (h *HandlerMaintenance) ExpireSnapshots(cttx sqlc.Tx, input *ExpireSnapshotsInput) (httpserver.Response, error) {
	var err error
	var taskId int64

	if taskId, err = h.serviceMaintenance.QueueExpireSnapshots(cttx, input.Table, input.RetentionDays, input.RetainLast); err != nil {
		return nil, fmt.Errorf("could not enqueue expire snapshots task: %w", err)
	}

	return httpserver.NewJsonResponse(&TaskQueuedResponse{
		TaskId: taskId,
		Status: "queued",
	}), nil
}

func (h *HandlerMaintenance) RemoveOrphanFiles(cttx sqlc.Tx, input *RemoveOrphanFilesInput) (httpserver.Response, error) {
	var err error
	var taskId int64

	if taskId, err = h.serviceMaintenance.QueueRemoveOrphanFiles(cttx, input.Table, input.RetentionDays); err != nil {
		return nil, fmt.Errorf("could not enqueue remove orphan files task: %w", err)
	}

	return httpserver.NewJsonResponse(&TaskQueuedResponse{
		TaskId: taskId,
		Status: "queued",
	}), nil
}

func (h *HandlerMaintenance) Optimize(cttx sqlc.Tx, input *OptimizeInput) (httpserver.Response, error) {
	var err error
	var taskIds []int64

	if taskIds, err = h.serviceMaintenance.QueueOptimize(cttx, input.Table, input.FileSizeThresholdMb, input.From.Time, input.To.Time); err != nil {
		return nil, fmt.Errorf("could not enqueue optimize tasks: %w", err)
	}

	return httpserver.NewJsonResponse(&OptimizeTaskQueuedResponse{
		TaskIds: taskIds,
		Status:  "queued",
	}), nil
}

func (h *HandlerMaintenance) ListTasks(cttx sqlc.Tx, input *ListTasksInput) (httpserver.Response, error) {
	var err error
	var result *PaginatedMaintenanceTask

	if result, err = h.serviceMaintenance.ListTasks(cttx, input.Table, input.Kind, input.Status, input.Limit, input.Offset); err != nil {
		return nil, fmt.Errorf("could not list tasks: %w", err)
	}

	return httpserver.NewJsonResponse(result), nil
}
