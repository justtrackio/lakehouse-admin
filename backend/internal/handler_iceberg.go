package internal

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gosoline-project/httpserver"
	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewHandlerIceberg(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerIceberg, error) {
	var err error
	var service *ServiceIceberg
	var iceberg *IcebergClient
	var refresh *ServiceRefresh
	var sqlClient sqlc.Client
	var files *ServiceFileIntegrity

	if service, err = NewServiceIceberg(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create iceberg service: %w", err)
	}

	if iceberg, err = ProvideIcebergClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create iceberg client: %w", err)
	}

	if refresh, err = NewServiceRefresh(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create refresh service: %w", err)
	}

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sql client: %w", err)
	}

	if files, err = NewServiceFileIntegrity(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create file integrity service: %w", err)
	}

	return &HandlerIceberg{
		logger:    logger.WithChannel("iceberg_handler"),
		service:   service,
		iceberg:   iceberg,
		refresh:   refresh,
		sqlClient: sqlClient,
		files:     files,
	}, nil
}

type HandlerIceberg struct {
	logger    log.Logger
	service   *ServiceIceberg
	iceberg   *IcebergClient
	refresh   *ServiceRefresh
	sqlClient sqlc.Client
	files     *ServiceFileIntegrity
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
	SnapshotID string `uri:"snapshotId"`
	// ExpectedSnapshotID is the main snapshot the user saw; the rollback only
	// commits while main is still there.
	ExpectedSnapshotID string `json:"expected_snapshot_id"`
}

type SnapshotMissingFilesResponse struct {
	SnapshotID   int64    `json:"snapshot_id,string"`
	MissingFiles []string `json:"missing_files"`
}

type SnapshotRollbackResponse struct {
	SnapshotID string `json:"snapshot_id"`
	// Status is rolled_back, or already_current when main was at the snapshot.
	Status string `json:"status"`
	// RefreshError is set when the rollback stands but reloading the stored
	// metadata failed; repeating the request only repeats the refresh.
	RefreshError string `json:"refresh_error,omitempty"`
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
	var err error
	var missingFiles []string

	if missingFiles, err = h.files.ListMissingFiles(ctx, input.Database, input.Table, input.SnapshotID); err != nil {
		return nil, fmt.Errorf("could not list missing files for snapshot %d: %w", input.SnapshotID, err)
	}

	return httpserver.NewJsonResponse(SnapshotMissingFilesResponse{
		SnapshotID:   input.SnapshotID,
		MissingFiles: missingFiles,
	}), nil
}

func (h *HandlerIceberg) RollbackToSnapshot(ctx context.Context, input *SnapshotRollbackInput) (httpserver.Response, error) {
	var err error
	var targetID, expectedID int64
	var committed bool

	if targetID, err = parseSnapshotID("snapshot id", input.SnapshotID); err != nil {
		return httpserver.GetErrorHandler()(http.StatusBadRequest, err), nil
	}

	if expectedID, err = parseSnapshotID("expected_snapshot_id", input.ExpectedSnapshotID); err != nil {
		return httpserver.GetErrorHandler()(http.StatusBadRequest, err), nil
	}

	committed, err = h.iceberg.RollbackToSnapshot(ctx, input.Database, input.Table, targetID, expectedID)
	switch {
	case errors.Is(err, ErrRollbackConflict):
		// Reload the stored metadata, so the page shows the head that moved.
		if refreshErr := h.refreshTable(ctx, input.Database, input.Table); refreshErr != nil {
			h.logger.Warn(ctx, "could not refresh table %s.%s after a rollback conflict: %s", input.Database, input.Table, refreshErr)
		}

		return httpserver.GetErrorHandler()(http.StatusConflict, err), nil
	case errors.Is(err, ErrRollbackInvalidTarget):
		return httpserver.GetErrorHandler()(http.StatusBadRequest, err), nil
	case err != nil:
		return nil, fmt.Errorf("could not rollback table %s.%s to snapshot %d: %w", input.Database, input.Table, targetID, err)
	}

	response := SnapshotRollbackResponse{SnapshotID: input.SnapshotID, Status: "already_current"}
	if committed {
		response.Status = "rolled_back"
		h.logger.Info(ctx, "rolled back table %s.%s from snapshot %d to %d", input.Database, input.Table, expectedID, targetID)
	}

	if err = h.refreshTable(ctx, input.Database, input.Table); err != nil {
		h.logger.Warn(ctx, "rolled back table %s.%s, but could not refresh its metadata: %s", input.Database, input.Table, err)
		response.RefreshError = err.Error()
	}

	return httpserver.NewJsonResponse(response), nil
}

func (h *HandlerIceberg) refreshTable(ctx context.Context, database string, table string) error {
	return h.sqlClient.WithTx(ctx, func(cttx sqlc.Tx) error {
		return h.refresh.RefreshTableFull(cttx, database, table)
	})
}

func parseSnapshotID(name string, value string) (int64, error) {
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", name, value)
	}

	return id, nil
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
	var err error
	var databases []CatalogDatabase

	if databases, err = h.service.ListDatabases(ctx); err != nil {
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
