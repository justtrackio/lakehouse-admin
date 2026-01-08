package main

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewHandlerRefresh(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerRefresh, error) {
	var err error
	var service *ServiceRefresh

	if service, err = NewServiceRefresh(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create spark client: %w", err)
	}

	return &HandlerRefresh{
		service: service,
	}, nil
}

type HandlerRefresh struct {
	service *ServiceRefresh
}

func (h *HandlerRefresh) RefreshTables(ctx context.Context) (httpserver.Response, error) {
	if _, err := h.service.RefreshAllTables(ctx); err != nil {
		return nil, fmt.Errorf("could not refresh all tables: %w", err)
	}

	return httpserver.NewJsonResponse(map[string]string{"status": "ok"}), nil
}

func (h *HandlerRefresh) RefreshTable(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	var err error

	if err = h.service.RefreshTableFull(ctx, input.Table); err != nil {
		return nil, fmt.Errorf("could not refresh table: %w", err)
	}

	return httpserver.NewJsonResponse(map[string]string{"status": "ok"}), nil
}

func (h *HandlerRefresh) RefreshPartitions(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	var err error
	var partitions []Partition

	if partitions, err = h.service.RefreshPartitions(ctx, input.Table); err != nil {
		return nil, fmt.Errorf("could not list snapshots: %w", err)
	}

	return httpserver.NewJsonResponse(partitions), nil
}

func (h *HandlerRefresh) RefreshSnapshots(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	var err error
	var snapshots []Snapshot

	if snapshots, err = h.service.RefreshSnapshots(ctx, input.Table); err != nil {
		return nil, fmt.Errorf("could not refresh snapshots: %w", err)
	}

	return httpserver.NewJsonResponse(snapshots), nil
}

func (h *HandlerRefresh) RefreshFull(ctx context.Context) (httpserver.Response, error) {
	if _, err := h.service.RefreshFull(ctx); err != nil {
		return nil, fmt.Errorf("could not complete full refresh: %w", err)
	}

	return httpserver.NewJsonResponse(map[string]string{"status": "ok"}), nil
}
