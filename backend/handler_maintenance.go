package main

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

const minRetentionDays = 7
const minRetainLast = 10

type ExpireSnapshotsInput struct {
	Table         string `uri:"table"`
	RetentionDays int    `json:"retention_days"`
	RetainLast    int    `json:"retain_last"`
}

type RemoveOrphanFilesInput struct {
	Table              string `uri:"table"`
	RetentionThreshold string `json:"retention_threshold"`
}

func NewHandlerMaintenance(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerMaintenance, error) {
	var err error
	var serviceMaintenance *ServiceMaintenance
	var serviceRefresh *ServiceRefresh

	if serviceMaintenance, err = NewServiceMaintenance(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create maintenance service: %w", err)
	}

	if serviceRefresh, err = NewServiceRefresh(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create maintenance service: %w", err)
	}

	return &HandlerMaintenance{
		serviceMaintenance: serviceMaintenance,
		serviceRefresh:     serviceRefresh,
	}, nil
}

type HandlerMaintenance struct {
	serviceMaintenance *ServiceMaintenance
	serviceRefresh     *ServiceRefresh
}

func (h *HandlerMaintenance) ExpireSnapshots(ctx context.Context, input *ExpireSnapshotsInput) (httpserver.Response, error) {
	var err error
	var result *ExpireSnapshotsResult

	// Handle retention days logic
	retentionDays := input.RetentionDays
	if retentionDays < minRetentionDays {
		retentionDays = minRetentionDays
	}

	// Set default if missing (though json decoder usually defaults to 0)
	retainLast := input.RetainLast
	if retainLast < minRetainLast {
		retainLast = minRetainLast
	}

	if result, err = h.serviceMaintenance.ExpireSnapshots(ctx, input.Table, retentionDays, retainLast); err != nil {
		return nil, fmt.Errorf("could not expire snapshots: %w", err)
	}

	if _, err = h.serviceRefresh.RefreshSnapshots(ctx, input.Table); err != nil {
		return nil, fmt.Errorf("could not resfresh snapshots: %w", err)
	}

	return httpserver.NewJsonResponse(result), nil
}

func (h *HandlerMaintenance) RemoveOrphanFiles(ctx context.Context, input *RemoveOrphanFilesInput) (httpserver.Response, error) {
	var err error
	var result *RemoveOrphanFilesResult

	if result, err = h.serviceMaintenance.RemoveOrphanFiles(ctx, input.Table, input.RetentionThreshold); err != nil {
		return nil, fmt.Errorf("could not remove orphan files: %w", err)
	}

	return httpserver.NewJsonResponse(result), nil
}
