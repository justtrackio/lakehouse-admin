package internal

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
	var settings *IcebergSettings

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}

	if settings, err = ReadIcebergSettings(config); err != nil {
		return nil, fmt.Errorf("could not read iceberg settings: %w", err)
	}

	return &ServiceMetadata{
		sqlClient: sqlClient,
		settings:  settings,
	}, nil
}

type ServiceMetadata struct {
	sqlClient sqlc.Client
	settings  *IcebergSettings
}

func (s *ServiceMetadata) GetTableSummary(ctx context.Context, desc TableDescription) (*TableSummary, error) {
	summary := &TableSummary{
		Database:          desc.Database,
		Name:              desc.Name,
		Partitions:        desc.Partitions.Get(),
		CurrentSnapshotID: desc.CurrentSnapshotID,
		UpdatedAt:         desc.UpdatedAt,
	}

	sel := s.sqlClient.Q().From("partitions").As("p").
		Column(sqlc.Col("*").Count().As("partition_count")).
		Column(sqlc.Coalesce(sqlc.Col("p.file_count").Sum(), 0).As("file_count")).
		Column(sqlc.Coalesce(sqlc.Col("p.record_count").Sum(), 0).As("record_count")).
		Column(sqlc.Coalesce(sqlc.Col("p.total_data_file_size_in_bytes").Sum(), 0).As("total_data_file_size_in_bytes")).
		Column(sqlc.Coalesce(sqlc.Col("p.needs_optimize").Max(), false).As("needs_optimize")).
		Where(sqlc.Eq{"p.database": desc.Database, "p.table": desc.Name})

	if err := sel.Get(ctx, summary); err != nil {
		return nil, fmt.Errorf("could not get partition summary: %w", err)
	}

	sel = s.sqlClient.Q().From("snapshots").As("s").
		Column(sqlc.Col("*").Count().As("snapshot_count")).
		Where(sqlc.Eq{"s.database": desc.Database, "s.table": desc.Name})

	if err := sel.Get(ctx, summary); err != nil {
		return nil, fmt.Errorf("could not get snapshot summary: %w", err)
	}

	return summary, nil
}

func (s *ServiceMetadata) GetTable(ctx context.Context, database string, name string) (*TableDescription, error) {
	database = s.resolveDatabase(database)

	table := &TableDescription{}
	if err := s.sqlClient.Q().From("tables").Where(sqlc.Eq{"database": database, "name": name}).Get(ctx, table); err != nil {
		return nil, fmt.Errorf("could not list tables from db: %w", err)
	}

	return table, nil
}

func (s *ServiceMetadata) ListTables(ctx context.Context, database string) ([]TableDescription, error) {
	database = s.resolveDatabase(database)

	tables := make([]TableDescription, 0)

	if err := s.sqlClient.Q().From("tables").Where(sqlc.Eq{"database": database}).OrderBy(sqlc.Col("name").Asc()).Select(ctx, &tables); err != nil {
		return nil, fmt.Errorf("could not list tables from db: %w", err)
	}

	return tables, nil
}

func (s *ServiceMetadata) ListAllTables(ctx context.Context) ([]TableDescription, error) {
	tables := make([]TableDescription, 0)

	if err := s.sqlClient.Q().From("tables").OrderBy(sqlc.Col("database").Asc()).OrderBy(sqlc.Col("name").Asc()).Select(ctx, &tables); err != nil {
		return nil, fmt.Errorf("could not list tables from db: %w", err)
	}

	return tables, nil
}

func (s *ServiceMetadata) resolveDatabase(database string) string {
	if database != "" {
		return database
	}

	return s.settings.DefaultDatabase
}
