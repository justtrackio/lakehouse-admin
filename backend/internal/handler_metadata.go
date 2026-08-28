package internal

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type UpdateTableMaintenanceSettingsInput struct {
	Database                  string `uri:"database"`
	Table                     string `uri:"table"`
	OptimizeDisabled          bool   `json:"optimize_disabled"`
	ExpireSnapshotsDisabled   bool   `json:"expire_snapshots_disabled"`
	RemoveOrphanFilesDisabled bool   `json:"remove_orphan_files_disabled"`
}

type UpdateTableMaintenanceSettingsResponse struct {
	TableMaintenanceSettings
	CancelledTaskCount int64 `json:"cancelled_task_count"`
}

type TableSelectInput struct {
	Database string `uri:"database"`
	Table    string `uri:"table"`
}

func NewHandlerMetadata(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerMetadata, error) {
	var err error
	var sqlClient sqlc.Client
	var tableMaintenance *ServiceTableMaintenance

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}
	if tableMaintenance, err = NewServiceTableMaintenance(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create table maintenance service: %w", err)
	}

	return &HandlerMetadata{
		sqlClient:        sqlClient,
		tableMaintenance: tableMaintenance,
	}, nil
}

type HandlerMetadata struct {
	sqlClient        sqlc.Client
	tableMaintenance *ServiceTableMaintenance
}

func (h *HandlerMetadata) UpdateTableMaintenanceSettings(ctx context.Context, input *UpdateTableMaintenanceSettingsInput) (httpserver.Response, error) {
	var err error
	var cancelled int64

	settings := TableMaintenanceSettings{
		OptimizeDisabled:          input.OptimizeDisabled,
		ExpireSnapshotsDisabled:   input.ExpireSnapshotsDisabled,
		RemoveOrphanFilesDisabled: input.RemoveOrphanFilesDisabled,
	}
	if cancelled, err = h.tableMaintenance.Update(ctx, input.Database, input.Table, settings); err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(&UpdateTableMaintenanceSettingsResponse{
		TableMaintenanceSettings: settings,
		CancelledTaskCount:       cancelled,
	}), nil
}

func (h *HandlerMetadata) ListPartitions(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	result := make([]Partition, 0)
	sel := h.sqlClient.Q().From("partitions").Where(sqlc.Eq{"database": input.Database, "table": input.Table})

	if err := sel.Select(ctx, &result); err != nil {
		return nil, fmt.Errorf("could not list partitions from db: %w", err)
	}

	return httpserver.NewJsonResponse(result), nil
}

func (h *HandlerMetadata) ListSnapshots(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	result := make([]Snapshot, 0)
	sel := h.sqlClient.Q().From("snapshots").Where(sqlc.Eq{"database": input.Database, "table": input.Table})

	if err := sel.Select(ctx, &result); err != nil {
		return nil, fmt.Errorf("could not list partitions from db: %w", err)
	}

	return httpserver.NewJsonResponse(result), nil
}
