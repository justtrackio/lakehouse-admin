package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/db"
	"github.com/justtrackio/gosoline/pkg/funk"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewServiceRefresh(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceRefresh, error) {
	var err error
	var iceberg *ServiceIceberg
	var sqlClient sqlc.Client

	if iceberg, err = NewServiceIceberg(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create iceberg service: %w", err)
	}

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}

	return &ServiceRefresh{
		logger:    logger.WithChannel("refresh"),
		iceberg:   iceberg,
		sqlClient: sqlClient,
	}, nil
}

type ServiceRefresh struct {
	logger    log.Logger
	iceberg   *ServiceIceberg
	sqlClient sqlc.Client
}

func (s *ServiceRefresh) LastUpdatedAt(ctx context.Context, name string) (time.Time, error) {
	table := &TableDescription{}
	if err := s.sqlClient.Q().From("tables").Where(sqlc.Eq{"name": name}).Get(ctx, table); err != nil {
		return time.Time{}, fmt.Errorf("could not get table description for table %s: %w", name, err)
	}

	return table.UpdatedAt, nil
}

func (s *ServiceRefresh) ListTables(ctx context.Context) ([]string, error) {
	return s.iceberg.ListTables(ctx)
}

func (s *ServiceRefresh) RefreshAllTables(ctx context.Context) ([]string, error) {
	var err error
	var tables []string

	if tables, err = s.iceberg.ListTables(ctx); err != nil {
		return nil, fmt.Errorf("could not list tables: %w", err)
	}

	for _, table := range tables {
		if _, err = s.RefreshTable(ctx, table); err != nil {
			return nil, fmt.Errorf("could not refresh table %s: %w", table, err)
		}
	}

	return tables, nil
}

func (s *ServiceRefresh) RefreshTable(ctx context.Context, table string) (*TableDescription, error) {
	var err error
	var desc *TableDescription

	if desc, err = s.iceberg.DescribeTable(ctx, table); err != nil {
		return nil, fmt.Errorf("could not describe table: %w", err)
	}

	insert := s.sqlClient.Q().Into("tables").Records(desc).Replace()
	if _, err = insert.Exec(ctx); err != nil {
		return nil, fmt.Errorf("could not save table description: %w", err)
	}

	s.logger.Info(ctx, "refreshed table %s", table)

	return desc, nil
}

func (s *ServiceRefresh) RefreshPartitions(ctx context.Context, table string) ([]Partition, error) {
	var err error
	var result []IcebergPartition

	if _, err = s.sqlClient.Q().Delete("partitions").Where(sqlc.Eq{"table": table}).Exec(ctx); err != nil {
		return nil, fmt.Errorf("could not delete existing partitions: %w", err)
	}

	if result, err = s.iceberg.ListPartitions(ctx, table); err != nil {
		return nil, fmt.Errorf("could not list partitions: %w", err)
	}

	partitions := make([]Partition, len(result))
	for i, p := range result {
		partitions[i] = Partition{
			Table:                    table,
			Partition:                db.NewJSON(p.Partition, db.NonNullable{}),
			SpecId:                   int(p.SpecID),
			RecordCount:              p.RecordCount,
			FileCount:                p.FileCount,
			TotalDataFileSizeInBytes: p.DataFileSizeBytes,
			LastUpdatedAt:            p.LastUpdatedAt,
			LastUpdatedSnapshotId:    p.LastSnapshotID,
		}
	}

	chunks := funk.Chunk(partitions, 100)
	for _, chunk := range chunks {
		insert := s.sqlClient.Q().Into("partitions").Records(chunk).Replace()

		if _, err = insert.Exec(ctx); err != nil {
			return nil, fmt.Errorf("could not save partitions: %w", err)
		}
	}

	s.logger.Info(ctx, "refreshed %d partitions for table %s", len(partitions), table)

	return partitions, nil
}

func (s *ServiceRefresh) RefreshSnapshots(ctx context.Context, table string) ([]Snapshot, error) {
	var err error
	var result []IcebergSnapshot

	if _, err = s.sqlClient.Q().Delete("snapshots").Where(sqlc.Eq{"table": table}).Exec(ctx); err != nil {
		return nil, fmt.Errorf("could not delete existing snapshots: %w", err)
	}

	if result, err = s.iceberg.ListSnapshots(ctx, table); err != nil {
		return nil, fmt.Errorf("could not list snapshots: %w", err)
	}

	snapshots := make([]Snapshot, len(result))
	for i := range result {
		snapshots[i].Table = table
		snapshots[i].CommittedAt = result[i].CommittedAt
		snapshots[i].SnapshotId = result[i].SnapshotID
		snapshots[i].ParentId = result[i].ParentID
		snapshots[i].Operation = result[i].Operation
		snapshots[i].ManifestList = result[i].ManifestList
		snapshots[i].Summary = db.NewJSON(result[i].Summary, db.NonNullable{})
	}

	chunks := funk.Chunk(snapshots, 100)
	for _, chunk := range chunks {
		insert := s.sqlClient.Q().Into("snapshots").Replace().Records(chunk)

		if _, err = insert.Exec(ctx); err != nil {
			return nil, fmt.Errorf("could not save snapshots: %w", err)
		}
	}

	s.logger.Info(ctx, "refreshed %d snapshots for table %s", len(snapshots), table)

	return snapshots, nil
}

func (s *ServiceRefresh) RefreshFull(ctx context.Context) ([]string, error) {
	var err error
	var tables []string

	if tables, err = s.iceberg.ListTables(ctx); err != nil {
		return nil, fmt.Errorf("could not list tables: %w", err)
	}

	s.logger.Info(ctx, "starting full refresh for %d tables", len(tables))

	for _, table := range tables {
		if err = s.RefreshTableFull(ctx, table); err != nil {
			return nil, fmt.Errorf("could not refresh table %s: %w", table, err)
		}
	}

	s.logger.Info(ctx, "completed full refresh for %d tables", len(tables))

	return tables, nil
}

func (s *ServiceRefresh) RefreshTableFull(ctx context.Context, table string) error {
	var err error

	s.logger.Info(ctx, "refreshing table %s", table)

	if _, err = s.RefreshTable(ctx, table); err != nil {
		return fmt.Errorf("could not refresh table %s: %w", table, err)
	}

	if _, err = s.RefreshPartitions(ctx, table); err != nil {
		return fmt.Errorf("could not refresh partitions for table %s: %w", table, err)
	}

	if _, err = s.RefreshSnapshots(ctx, table); err != nil {
		return fmt.Errorf("could not refresh snapshots for table %s: %w", table, err)
	}

	return nil
}
