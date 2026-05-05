package internal

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewHandlerIceberg(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerIceberg, error) {
	var err error
	var service *ServiceIceberg
	var admin *ServiceIcebergAdmin
	var files *ServiceFileIntegrity
	var refresh *ServiceRefresh
	var sqlClient sqlc.Client

	if service, err = NewServiceIceberg(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create iceberg service: %w", err)
	}

	if admin, err = NewServiceIcebergAdmin(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create iceberg admin service: %w", err)
	}

	if files, err = NewServiceFileIntegrity(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create file integrity service: %w", err)
	}

	if refresh, err = NewServiceRefresh(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create refresh service: %w", err)
	}

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sql client: %w", err)
	}

	return &HandlerIceberg{
		service:   service,
		admin:     admin,
		files:     files,
		refresh:   refresh,
		sqlClient: sqlClient,
	}, nil
}

type HandlerIceberg struct {
	service   *ServiceIceberg
	admin     *ServiceIcebergAdmin
	files     *ServiceFileIntegrity
	refresh   *ServiceRefresh
	sqlClient sqlc.Client
}

type IcebergListSnapshotsResponse struct {
	Snapshots []IcebergSnapshot `json:"snapshots"`
}

type IcebergListPartitionsResponse struct {
	Partitions []IcebergPartition `json:"partitions"`
}

type SnapshotMissingFilesInput struct {
	Database   string `uri:"database"`
	Table      string `uri:"table"`
	SnapshotID int64  `uri:"snapshotId"`
}

type SnapshotRollbackInput struct {
	Database   string `uri:"database"`
	Table      string `uri:"table"`
	SnapshotID int64  `uri:"snapshotId"`
}

type SnapshotMissingFilesResponse struct {
	SnapshotID   int64    `json:"snapshot_id,string"`
	MissingFiles []string `json:"missing_files"`
}

type SnapshotRollbackResponse struct {
	SnapshotID int64  `json:"snapshot_id,string"`
	Status     string `json:"status"`
}

func (h *HandlerIceberg) ListSnapshots(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	var err error
	var snapshots []IcebergSnapshot

	if snapshots, err = h.service.ListSnapshots(ctx, input.Database, input.Table); err != nil {
		return nil, fmt.Errorf("could not list snapshots: %w", err)
	}

	return httpserver.NewJsonResponse(IcebergListSnapshotsResponse{
		Snapshots: snapshots,
	}), nil
}

func (h *HandlerIceberg) ListPartitions(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	var err error
	var partitions []IcebergPartition

	if partitions, err = h.service.ListPartitions(ctx, input.Database, input.Table); err != nil {
		return nil, fmt.Errorf("could not list partitions: %w", err)
	}

	return httpserver.NewJsonResponse(IcebergListPartitionsResponse{
		Partitions: partitions,
	}), nil
}

func (h *HandlerIceberg) ListSnapshotMissingFiles(ctx context.Context, input *SnapshotMissingFilesInput) (httpserver.Response, error) {
	missingFiles, err := h.files.ListMissingFiles(ctx, input.Database, input.Table, input.SnapshotID)
	if err != nil {
		return nil, fmt.Errorf("could not list missing files for snapshot %d: %w", input.SnapshotID, err)
	}

	return httpserver.NewJsonResponse(SnapshotMissingFilesResponse{
		SnapshotID:   input.SnapshotID,
		MissingFiles: missingFiles,
	}), nil
}

func (h *HandlerIceberg) RollbackToSnapshot(ctx context.Context, input *SnapshotRollbackInput) (httpserver.Response, error) {
	if err := h.admin.RollbackToSnapshot(ctx, input.Database, input.Table, input.SnapshotID); err != nil {
		return nil, fmt.Errorf("could not rollback table %s.%s to snapshot %d: %w", input.Database, input.Table, input.SnapshotID, err)
	}

	if err := h.sqlClient.WithTx(ctx, func(cttx sqlc.Tx) error {
		if err := h.refresh.RefreshTableFull(cttx, input.Database, input.Table); err != nil {
			return fmt.Errorf("could not refresh table metadata after rollback: %w", err)
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("could not refresh table %s.%s after rollback to snapshot %d: %w", input.Database, input.Table, input.SnapshotID, err)
	}

	return httpserver.NewJsonResponse(SnapshotRollbackResponse{
		SnapshotID: input.SnapshotID,
		Status:     statusOK,
	}), nil
}

func (h *HandlerIceberg) ListTables(ctx context.Context, input *DatabaseInput) (httpserver.Response, error) {
	var err error
	var tables []CatalogTable

	if tables, err = h.service.ListTables(ctx, input.Database); err != nil {
		return nil, fmt.Errorf("could not list tables: %w", err)
	}

	return httpserver.NewJsonResponse(tables), nil
}

func (h *HandlerIceberg) ListDatabases(ctx context.Context) (httpserver.Response, error) {
	databases, err := h.service.ListDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not list databases: %w", err)
	}

	return httpserver.NewJsonResponse(CatalogDatabasesResponse{
		Databases:       databases,
		DefaultDatabase: h.service.settings.DefaultDatabase,
	}), nil
}

func (h *HandlerIceberg) DescribeTable(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	var err error
	var desc *TableDescription

	if desc, err = h.service.DescribeTable(ctx, input.Database, input.Table); err != nil {
		return nil, fmt.Errorf("could not describe table: %w", err)
	}

	return httpserver.NewJsonResponse(desc), nil
}
