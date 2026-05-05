package internal

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gosoline-project/httpserver"
	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func partitionJSONPathExpr(key string, text bool) string {
	operator := "->"
	if text {
		operator = "->>"
	}

	return fmt.Sprintf("p.partition%s'$.%q'", operator, key)
}

type ListTablesResponse struct {
	Tables []*TableSummary `json:"tables"`
}

type DatabaseInput struct {
	Database string `uri:"database"`
}

type ListPartitionsResponse struct {
	Partitions []ListPartitionItem `json:"partitions"`
}

type ListFilesResponse struct {
	Files []DataFileItem `json:"files"`
}

type ListPartitionItem struct {
	Name                     string `json:"name" db:"name"`
	FileCount                int64  `json:"file_count" db:"file_count"`
	RecordCount              int64  `json:"record_count" db:"record_count"`
	TotalDataFileSizeInBytes int64  `json:"total_data_file_size_in_bytes" db:"total_data_file_size_in_bytes"`
	NeedsOptimize            bool   `json:"needs_optimize" db:"needs_optimize"`
	NeedsOptimizeCount       int64  `json:"needs_optimize_count" db:"needs_optimize_count"`
}

type DataFileItem struct {
	Content         int64  `json:"content"`
	FilePath        string `json:"file_path"`
	FileFormat      string `json:"file_format"`
	SpecID          int64  `json:"spec_id"`
	Partition       string `json:"partition"`
	RecordCount     int64  `json:"record_count"`
	FileSizeInBytes int64  `json:"file_size_in_bytes"`
}

type ListPartitionsInput struct {
	Database   string            `uri:"database"`
	Table      string            `uri:"table"`
	Partitions map[string]string `form:"partitions"`
}

type ListFilesInput struct {
	Database   string            `uri:"database"`
	Table      string            `uri:"table"`
	Partitions map[string]string `json:"partitions" form:"partitions"`
}

func NewHandlerBrowse(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerBrowse, error) {
	var err error
	var sqlClient sqlc.Client
	var metadata *ServiceMetadata
	var files *ServiceBrowseFiles

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}

	if metadata, err = NewServiceMetadata(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create service metadata: %w", err)
	}

	if files, err = NewServiceBrowseFiles(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create file browse service: %w", err)
	}

	return &HandlerBrowse{
		sqlClient: sqlClient,
		metadata:  metadata,
		files:     files,
	}, nil
}

type HandlerBrowse struct {
	sqlClient sqlc.Client
	metadata  *ServiceMetadata
	files     *ServiceBrowseFiles
}

func (h *HandlerBrowse) TableSummary(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	var err error
	var table *TableDescription
	var summary *TableSummary

	if table, err = h.metadata.GetTable(ctx, input.Database, input.Table); err != nil {
		return nil, fmt.Errorf("could not describe table: %w", err)
	}

	if summary, err = h.metadata.GetTableSummary(ctx, *table); err != nil {
		return nil, fmt.Errorf("could not describe table summary: %w", err)
	}

	return httpserver.NewJsonResponse(summary), nil
}

func (h *HandlerBrowse) ListTables(ctx context.Context, input *DatabaseInput) (httpserver.Response, error) {
	var err error
	var tables []TableDescription

	if tables, err = h.metadata.ListTables(ctx, input.Database); err != nil {
		return nil, fmt.Errorf("could not list tables from db: %w", err)
	}

	items := make([]*TableSummary, len(tables))
	for i, table := range tables {
		if items[i], err = h.metadata.GetTableSummary(ctx, table); err != nil {
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

	if table, err = h.metadata.GetTable(ctx, input.Database, input.Table); err != nil {
		return nil, fmt.Errorf("could not describe table: %w", err)
	}

	partitions := table.Partitions.Get()
	depth := len(input.Partitions)

	if len(partitions) == 0 || depth >= len(partitions) {
		return httpserver.NewJsonResponse(ListPartitionsResponse{Partitions: []ListPartitionItem{}}), nil
	}

	groupBy := partitionJSONPathExpr(partitions[depth].Name, true)
	where := sqlc.Eq{"p.database": input.Database, "p.table": input.Table}

	for key, value := range input.Partitions {
		where[partitionJSONPathExpr(key, false)] = value
	}

	sel := h.sqlClient.Q().From("partitions").As("p").
		Column(sqlc.Col(groupBy).As("name")).
		Column(sqlc.Col("p.file_count").Sum().As("file_count")).
		Column(sqlc.Col("p.record_count").Sum().As("record_count")).
		Column(sqlc.Col("p.total_data_file_size_in_bytes").Sum().As("total_data_file_size_in_bytes")).
		Column(sqlc.Coalesce(sqlc.Col("p.needs_optimize").Max(), false).As("needs_optimize")).
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

func (h *HandlerBrowse) ListFiles(ctx context.Context, input *ListFilesInput) (httpserver.Response, error) {
	var err error
	var items []DataFileItem

	if items, err = h.files.ListFiles(ctx, input.Database, input.Table, input.Partitions); err != nil {
		if isBrowseInputError(err) {
			return httpserver.GetErrorHandler()(http.StatusBadRequest, err), nil
		}

		return nil, fmt.Errorf("could not list files: %w", err)
	}

	return httpserver.NewJsonResponse(ListFilesResponse{Files: items}), nil
}
