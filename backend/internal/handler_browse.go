package internal

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type ListTablesResponse struct {
	Tables []*TableSummary `json:"tables"`
}

type ListPartitionsResponse struct {
	Partitions []ListPartitionItem `json:"partitions"`
}

type ListPartitionItem struct {
	Name                     string `json:"name" db:"name"`
	FileCount                int64  `json:"file_count" db:"file_count"`
	RecordCount              int64  `json:"record_count" db:"record_count"`
	TotalDataFileSizeInBytes int64  `json:"total_data_file_size_in_bytes" db:"total_data_file_size_in_bytes"`
	NeedsOptimize            bool   `json:"needs_optimize" db:"needs_optimize"`
	NeedsOptimizeCount       int64  `json:"needs_optimize_count" db:"needs_optimize_count"`
}

type ListPartitionsInput struct {
	Table      string            `uri:"table"`
	Partitions map[string]string `form:"partitions"`
}

func NewHandlerBrowse(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerBrowse, error) {
	var err error
	var sqlClient sqlc.Client
	var service *ServiceMetadata

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}

	if service, err = NewServiceMetadata(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create service metadata: %w", err)
	}

	return &HandlerBrowse{
		sqlClient: sqlClient,
		service:   service,
	}, nil
}

type HandlerBrowse struct {
	sqlClient sqlc.Client
	service   *ServiceMetadata
}

func (h *HandlerBrowse) TableSummary(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	var err error
	var table *TableDescription
	var summary *TableSummary

	if table, err = h.service.GetTable(ctx, input.Table); err != nil {
		return nil, fmt.Errorf("could not describe table: %w", err)
	}

	if summary, err = h.service.GetTableSummary(ctx, *table); err != nil {
		return nil, fmt.Errorf("could not describe table summary: %w", err)
	}

	return httpserver.NewJsonResponse(summary), nil
}

func (h *HandlerBrowse) ListTables(ctx context.Context) (httpserver.Response, error) {
	var err error
	var tables []TableDescription

	if tables, err = h.service.ListTables(ctx); err != nil {
		return nil, fmt.Errorf("could not list tables from db: %w", err)
	}

	items := make([]*TableSummary, len(tables))
	for i, table := range tables {
		if items[i], err = h.service.GetTableSummary(ctx, table); err != nil {
			return nil, fmt.Errorf("could not get table summary for table %s: %w", table.Name, err)
		}
	}

	return httpserver.NewJsonResponse(ListTablesResponse{
		Tables: items,
	}), nil
}

func (h *HandlerBrowse) ListPartitions(ctx context.Context, input *ListPartitionsInput) (httpserver.Response, error) {
	var err error
	var table *TableDescription

	if table, err = h.service.GetTable(ctx, input.Table); err != nil {
		return nil, fmt.Errorf("could not describe table: %w", err)
	}

	partitions := table.Partitions.Get()
	depth := len(input.Partitions)

	// Handle unpartitioned tables or out-of-range depth gracefully
	if len(partitions) == 0 || depth >= len(partitions) {
		return httpserver.NewJsonResponse(ListPartitionsResponse{
			Partitions: []ListPartitionItem{},
		}), nil
	}

	groupBy := partitions[depth].Name
	groupBy = fmt.Sprintf("p.partition->>'$.%s'", groupBy)

	where := sqlc.Eq{"p.table": input.Table}
	for k, v := range input.Partitions {
		expr := fmt.Sprintf("p.partition->'$.%s'", k)
		where[expr] = v
	}

	sel := h.sqlClient.Q().From("partitions").As("p").
		Column(sqlc.Col(groupBy).As("name")).
		Column(sqlc.Col("p.file_count").Sum().As("file_count")).
		Column(sqlc.Col("p.record_count").Sum().As("record_count")).
		Column(sqlc.Col("p.total_data_file_size_in_bytes").Sum().As("total_data_file_size_in_bytes")).
		Column(sqlc.Col("p.needs_optimize").Max().As("needs_optimize")).
		Column(sqlc.Col("p.needs_optimize").Sum().As("needs_optimize_count")).
		Where(where).
		GroupBy(sqlc.Lit(1)).
		OrderBy(sqlc.Lit(1).Asc())

	items := make([]ListPartitionItem, 0)
	if err = sel.Select(ctx, &items); err != nil {
		return nil, fmt.Errorf("could not execute table list query: %w", err)
	}

	return httpserver.NewJsonResponse(ListPartitionsResponse{
		Partitions: items,
	}), nil
}
