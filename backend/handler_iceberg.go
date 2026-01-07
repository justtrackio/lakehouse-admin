package main

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

	if service, err = NewServiceIceberg(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create iceberg service: %w", err)
	}

	return &HandlerIceberg{
		service: service,
	}, nil
}

type HandlerIceberg struct {
	service *ServiceIceberg
}

type IcebergListSnapshotsResponse struct {
	Snapshots []IcebergSnapshot `json:"snapshots"`
}

type IcebergListPartitionsResponse struct {
	Partitions []IcebergPartition `json:"partitions"`
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

func (h *HandlerIceberg) ListTables(ctx context.Context) (httpserver.Response, error) {
	var err error
	var tables []string

	if tables, err = h.service.ListTables(ctx); err != nil {
		return nil, fmt.Errorf("could not list tables: %w", err)
	}

	return httpserver.NewJsonResponse(tables), nil
}
