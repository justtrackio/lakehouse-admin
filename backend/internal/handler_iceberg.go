package internal

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gosoline-project/httpserver"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewHandlerIceberg(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerIceberg, error) {
	var err error
	var service *ServiceIceberg
	var tasks *ServiceTasks
	var files *ServiceFileIntegrity

	if service, err = NewServiceIceberg(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create iceberg service: %w", err)
	}

	if tasks, err = NewServiceTasks(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create tasks service: %w", err)
	}

	if files, err = NewServiceFileIntegrity(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create file integrity service: %w", err)
	}

	return &HandlerIceberg{
		service: service,
		tasks:   tasks,
		files:   files,
	}, nil
}

type HandlerIceberg struct {
	service *ServiceIceberg
	tasks   *ServiceTasks
	files   *ServiceFileIntegrity
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
	TaskId     int64  `json:"task_id"`
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
	var taskId int64

	if input.SnapshotID <= 0 {
		return httpserver.GetErrorHandler()(http.StatusBadRequest, fmt.Errorf("snapshot id must be positive, got %d", input.SnapshotID)), nil
	}

	if taskId, err = h.tasks.EnqueueRollbackToSnapshot(ctx, input.Database, input.Table, input.SnapshotID); err != nil {
		return nil, fmt.Errorf("could not rollback table %s.%s to snapshot %d: %w", input.Database, input.Table, input.SnapshotID, err)
	}

	return httpserver.NewJsonResponse(SnapshotRollbackResponse{
		SnapshotID: input.SnapshotID,
		TaskId:     taskId,
		Status:     taskStatusQueued,
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
