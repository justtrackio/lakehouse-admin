package internal

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type BatchExpireSnapshotsInput struct {
	Tables        []string `json:"tables"`
	RetentionDays int      `json:"retention_days"`
	RetainLast    int      `json:"retain_last"`
}

type BatchRemoveOrphanFilesInput struct {
	Tables        []string `json:"tables"`
	RetentionDays int      `json:"retention_days"`
}

func NewHandlerMaintenance(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerMaintenance, error) {
	serviceTasks, err := NewServiceTasks(ctx, config, logger)
	if err != nil {
		return nil, fmt.Errorf("could not create maintenance service: %w", err)
	}

	return &HandlerMaintenance{
		serviceTasks: serviceTasks,
	}, nil
}

type HandlerMaintenance struct {
	serviceTasks *ServiceTasks
}

func (h *HandlerMaintenance) ExpireSnapshots(ctx context.Context, input *BatchExpireSnapshotsInput) (httpserver.Response, error) {
	result, err := h.serviceTasks.EnqueueExpireSnapshotsBatch(ctx, input.Tables, input.RetentionDays, input.RetainLast)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(result), nil
}

func (h *HandlerMaintenance) RemoveOrphanFiles(ctx context.Context, input *BatchRemoveOrphanFilesInput) (httpserver.Response, error) {
	result, err := h.serviceTasks.EnqueueRemoveOrphanFilesBatch(ctx, input.Tables, input.RetentionDays)
	if err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(result), nil
}
