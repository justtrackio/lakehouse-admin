package internal

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewHandlerIceberg(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerIceberg, error) {
	var err error
	var service *ServiceIceberg
	var files *ServiceFileIntegrity

	if service, err = NewServiceIceberg(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create iceberg service: %w", err)
	}

	if files, err = NewServiceFileIntegrity(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create file integrity service: %w", err)
	}

	return &HandlerIceberg{
		service: service,
		files:   files,
	}, nil
}

type HandlerIceberg struct {
	service *ServiceIceberg
	files   *ServiceFileIntegrity
}

type IcebergListSnapshotsResponse struct {
	Snapshots []IcebergSnapshot `json:"snapshots"`
}

type IcebergListPartitionsResponse struct {
	Partitions []IcebergPartition `json:"partitions"`
}

type SnapshotMissingFilesInput struct {
	Table      string `uri:"table"`
	SnapshotID int64  `uri:"snapshotId"`
}

type SnapshotMissingFilesResponse struct {
	SnapshotID   int64    `json:"snapshot_id,string"`
	MissingFiles []string `json:"missing_files"`
}

func (h *HandlerIceberg) ListSnapshots(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	var err error
	var snapshots []IcebergSnapshot

	if snapshots, err = h.service.ListSnapshots(ctx, input.Table); err != nil {
		return nil, fmt.Errorf("could not list snapshots: %w", err)
	}

	return httpserver.NewJsonResponse(IcebergListSnapshotsResponse{
		Snapshots: snapshots,
	}), nil
}

func (h *HandlerIceberg) ListPartitions(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	var err error
	var partitions []IcebergPartition

	if partitions, err = h.service.ListPartitions(ctx, input.Table); err != nil {
		return nil, fmt.Errorf("could not list partitions: %w", err)
	}

	return httpserver.NewJsonResponse(IcebergListPartitionsResponse{
		Partitions: partitions,
	}), nil
}

func (h *HandlerIceberg) ListSnapshotMissingFiles(ctx context.Context, input *SnapshotMissingFilesInput) (httpserver.Response, error) {
	missingFiles, err := h.files.ListMissingFiles(ctx, input.Table, input.SnapshotID)
	if err != nil {
		return nil, fmt.Errorf("could not list missing files for snapshot %d: %w", input.SnapshotID, err)
	}

	return httpserver.NewJsonResponse(SnapshotMissingFilesResponse{
		SnapshotID:   input.SnapshotID,
		MissingFiles: missingFiles,
	}), nil
}

func (h *HandlerIceberg) ListTables(ctx context.Context) (httpserver.Response, error) {
	var err error
	var tables []string

	if tables, err = h.service.ListTables(ctx); err != nil {
		return nil, fmt.Errorf("could not list tables: %w", err)
	}

	return httpserver.NewJsonResponse(tables), nil
}

func (h *HandlerIceberg) DescribeTable(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	var err error
	var desc *TableDescription

	if desc, err = h.service.DescribeTable(ctx, input.Table); err != nil {
		return nil, fmt.Errorf("could not describe table: %w", err)
	}

	return httpserver.NewJsonResponse(desc), nil
}
