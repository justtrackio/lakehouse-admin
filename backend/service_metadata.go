package main

import (
	"context"
	"fmt"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewServiceMetadata(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceMetadata, error) {
	var err error
	var sqlClient sqlc.Client

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}

	return &ServiceMetadata{
		sqlClient: sqlClient,
	}, nil
}

type ServiceMetadata struct {
	sqlClient sqlc.Client
}

func (s *ServiceMetadata) GetTableSummary(ctx context.Context, desc TableDescription) (*TableSummary, error) {
	summary := &TableSummary{
		Name:       desc.Name,
		Partitions: desc.Partitions.Get(),
	}

	sel := s.sqlClient.Q().From("partitions").As("p").
		Column(sqlc.Col("*").Count().As("partition_count")).
		Column(sqlc.Col("p.file_count").Sum().As("file_count")).
		Column(sqlc.Col("p.record_count").Sum().As("record_count")).
		Column(sqlc.Col("p.total_data_file_size_in_bytes").Sum().As("total_data_file_size_in_bytes")).
		Where(sqlc.Col("p.table").Eq(desc.Name))

	if err := sel.Get(ctx, summary); err != nil {
		return nil, fmt.Errorf("could not get partition summary: %w", err)
	}

	sel = s.sqlClient.Q().From("snapshots").As("s").
		Column(sqlc.Col("*").Count().As("snapshot_count")).
		Where(sqlc.Col("s.table").Eq(desc.Name))

	if err := sel.Get(ctx, summary); err != nil {
		return nil, fmt.Errorf("could not get snapshot summary: %w", err)
	}

	return summary, nil
}

func (s *ServiceMetadata) GetTable(ctx context.Context, name string) (*TableDescription, error) {
	table := &TableDescription{}
	if err := s.sqlClient.Q().From("tables").Where(sqlc.Eq{"name": name}).Get(ctx, table); err != nil {
		return nil, fmt.Errorf("could not list tables from db: %w", err)
	}

	return table, nil
}

func (s *ServiceMetadata) ListTables(ctx context.Context) ([]TableDescription, error) {
	tables := make([]TableDescription, 0)

	if err := s.sqlClient.Q().From("tables").Select(ctx, &tables); err != nil {
		return nil, fmt.Errorf("could not list tables from db: %w", err)
	}

	return tables, nil
}
