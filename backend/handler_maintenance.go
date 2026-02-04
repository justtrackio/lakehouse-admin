package main

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/gosoline-project/sqlc"
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
	Table         string `uri:"table"`
	RetentionDays int    `json:"retention_days"`
}

type OptimizeInput struct {
	Table               string   `uri:"table"`
	FileSizeThresholdMb int      `json:"file_size_threshold_mb"`
	From                DateTime `json:"from"`
	To                  DateTime `json:"to"`
	BatchSize           string   `json:"batch_size"`
}

type ListHistoryInput struct {
	Table  string `form:"table"`
	Limit  int    `form:"limit"`
	Offset int    `form:"offset"`
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

func (h *HandlerMaintenance) ExpireSnapshots(cttx sqlc.Tx, input *ExpireSnapshotsInput) (httpserver.Response, error) {
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

	if result, err = h.serviceMaintenance.ExpireSnapshots(cttx, input.Table, retentionDays, retainLast); err != nil {
		return nil, fmt.Errorf("could not expire snapshots: %w", err)
	}

	if _, err = h.serviceRefresh.RefreshSnapshots(cttx, input.Table); err != nil {
		return nil, fmt.Errorf("could not resfresh snapshots: %w", err)
	}

	return httpserver.NewJsonResponse(result), nil
}

func (h *HandlerMaintenance) RemoveOrphanFiles(cttx sqlc.Tx, input *RemoveOrphanFilesInput) (httpserver.Response, error) {
	var err error
	var result *RemoveOrphanFilesResult

	// Handle retention days logic
	retentionDays := input.RetentionDays
	if retentionDays < minRetentionDays {
		retentionDays = minRetentionDays
	}

	if result, err = h.serviceMaintenance.RemoveOrphanFiles(cttx, input.Table, retentionDays); err != nil {
		return nil, fmt.Errorf("could not remove orphan files: %w", err)
	}

	return httpserver.NewJsonResponse(result), nil
}

func (h *HandlerMaintenance) Optimize(cttx sqlc.Tx, input *OptimizeInput) (httpserver.Response, error) {
	var err error
	var result *OptimizeResult

	fileSizeThresholdMb := input.FileSizeThresholdMb
	if fileSizeThresholdMb < 1 {
		fileSizeThresholdMb = 128
	}

	batchSize := input.BatchSize
	if batchSize == "" {
		batchSize = "monthly"
	}

	// 1. Call Service Optimize (now handles all logic including metadata and where clause)
	if result, err = h.serviceMaintenance.Optimize(cttx, input.Table, fileSizeThresholdMb, input.From, input.To, batchSize); err != nil {
		return nil, fmt.Errorf("could not optimize table: %w", err)
	}

	// 2. Trigger Full Refresh
	if err = h.serviceRefresh.RefreshTableFull(cttx, input.Table); err != nil {
		return nil, fmt.Errorf("could not refresh table after optimize: %w", err)
	}

	return httpserver.NewJsonResponse(result), nil
}

func (h *HandlerMaintenance) ListHistory(cttx sqlc.Tx, input *ListHistoryInput) (httpserver.Response, error) {
	var err error
	var result *PaginatedMaintenanceHistory

	limit := input.Limit
	if limit <= 0 {
		limit = 20
	}

	offset := input.Offset
	if offset < 0 {
		offset = 0
	}

	if result, err = h.serviceMaintenance.ListHistory(cttx, input.Table, limit, offset); err != nil {
		return nil, fmt.Errorf("could not list history: %w", err)
	}

	return httpserver.NewJsonResponse(result), nil
}
