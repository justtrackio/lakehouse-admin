package main

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type TableSelectInput struct {
	Table string `form:"table" uri:"table"`
}

func NewHandlerMetadata(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerMetadata, error) {
	var err error
	var sqlClient sqlc.Client

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}

	return &HandlerMetadata{
		sqlClient: sqlClient,
	}, nil
}

type HandlerMetadata struct {
	sqlClient sqlc.Client
}

func (h *HandlerMetadata) ListPartitions(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	result := make([]Partition, 0)
	sel := h.sqlClient.Q().From("partitions").Where(sqlc.Col("table").Eq(input.Table))

	if err := sel.Select(ctx, &result); err != nil {
		return nil, fmt.Errorf("could not list partitions from db: %w", err)
	}

	return httpserver.NewJsonResponse(result), nil
}

func (h *HandlerMetadata) ListSnapshots(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	result := make([]Snapshot, 0)
	sel := h.sqlClient.Q().From("snapshots").Where(sqlc.Col("table").Eq(input.Table))

	if err := sel.Select(ctx, &result); err != nil {
		return nil, fmt.Errorf("could not list partitions from db: %w", err)
	}

	return httpserver.NewJsonResponse(result), nil
}
