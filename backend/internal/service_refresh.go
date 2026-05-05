package internal

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

type icebergRefresher interface {
	ListDatabases(ctx context.Context) ([]CatalogDatabase, error)
	ListTables(ctx context.Context, database string) ([]CatalogTable, error)
	DescribeTable(ctx context.Context, database string, logicalName string) (*TableDescription, error)
	ListPartitions(ctx context.Context, database string, logicalName string) ([]IcebergPartition, error)
	ListSnapshots(ctx context.Context, database string, logicalName string) ([]IcebergSnapshot, error)
}

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
	iceberg   icebergRefresher
	sqlClient sqlc.Client
}

func (s *ServiceRefresh) LastUpdatedAt(ctx context.Context, database string, name string) (time.Time, error) {
	table := &TableDescription{}
	if err := s.sqlClient.Q().From("tables").Where(sqlc.Eq{"database": database, "name": name}).Get(ctx, table); err != nil {
		return time.Time{}, fmt.Errorf("could not get table description for table %s.%s: %w", database, name, err)
	}

	return table.UpdatedAt, nil
}

func (s *ServiceRefresh) ListTables(ctx context.Context, database string) ([]CatalogTable, error) {
	return s.iceberg.ListTables(ctx, database)
}

func (s *ServiceRefresh) RefreshAllTables(cttx sqlc.Tx) ([]CatalogTable, error) {
	var err error
	var databases []CatalogDatabase

	if databases, err = s.iceberg.ListDatabases(cttx); err != nil {
		return nil, fmt.Errorf("could not list databases: %w", err)
	}

	var allTables []CatalogTable
	for _, database := range databases {
		icebergTables, err := s.iceberg.ListTables(cttx, database.Name)
		if err != nil {
			return nil, fmt.Errorf("could not list tables for database %s: %w", database.Name, err)
		}

		storedTables, err := s.listStoredTables(cttx, database.Name)
		if err != nil {
			return nil, fmt.Errorf("could not list stored tables for database %s: %w", database.Name, err)
		}

		_, staleTables := funk.Difference(icebergTables, storedTables)
		for _, table := range staleTables {
			if err = s.deleteStaleTable(cttx, table.Database, table.Name); err != nil {
				return nil, fmt.Errorf("could not delete stale table %s.%s: %w", table.Database, table.Name, err)
			}
		}

		s.logger.Info(cttx, "deleted %d stale tables from database %s", len(staleTables), database.Name)

		for _, table := range icebergTables {
			if _, err = s.RefreshTable(cttx, table.Database, table.Name); err != nil {
				return nil, fmt.Errorf("could not refresh table %s.%s: %w", table.Database, table.Name, err)
			}
		}

		allTables = append(allTables, icebergTables...)
	}

	return allTables, nil
}

func (s *ServiceRefresh) RefreshTable(cttx sqlc.Tx, database string, table string) (*TableDescription, error) {
	var err error
	var desc *TableDescription

	if desc, err = s.iceberg.DescribeTable(cttx, database, table); err != nil {
		return nil, fmt.Errorf("could not describe table: %w", err)
	}

	insert := cttx.Q().Into("tables").Records(desc).Replace()
	if _, err = insert.Exec(cttx); err != nil {
		return nil, fmt.Errorf("could not save table description: %w", err)
	}

	s.logger.Info(cttx, "refreshed table %s.%s", database, table)

	return desc, nil
}

func (s *ServiceRefresh) RefreshPartitions(cttx sqlc.Tx, database string, table string) ([]Partition, error) {
	var err error
	var result []IcebergPartition

	if _, err = cttx.Q().Delete("partitions").Where(sqlc.Eq{"database": database, "table": table}).Exec(cttx); err != nil {
		return nil, fmt.Errorf("could not delete existing partitions: %w", err)
	}

	if result, err = s.iceberg.ListPartitions(cttx, database, table); err != nil {
		return nil, fmt.Errorf("could not list partitions: %w", err)
	}

	partitions := make([]Partition, len(result))
	for i, p := range result {
		partitions[i] = Partition{
			Database:                 database,
			Table:                    table,
			Partition:                db.NewJSON(p.Partition, db.NonNullable{}),
			SpecId:                   int(p.SpecID),
			RecordCount:              p.RecordCount,
			FileCount:                p.FileCount,
			TotalDataFileSizeInBytes: p.DataFileSizeBytes,
			LastUpdatedAt:            p.LastUpdatedAt,
			LastUpdatedSnapshotId:    p.LastSnapshotID,
			NeedsOptimize:            p.NeedsOptimize,
		}
	}

	chunks := funk.Chunk(partitions, 100)
	for _, chunk := range chunks {
		insert := cttx.Q().Into("partitions").Records(chunk)

		if _, err = insert.Exec(cttx); err != nil {
			return nil, fmt.Errorf("could not save partitions: %w", err)
		}
	}

	s.logger.Info(cttx, "refreshed %d partitions for table %s.%s", len(partitions), database, table)

	return partitions, nil
}

func (s *ServiceRefresh) RefreshSnapshots(cttx sqlc.Tx, database string, table string) ([]Snapshot, error) {
	var err error
	var result []IcebergSnapshot

	if _, err = cttx.Q().Delete("snapshots").Where(sqlc.Eq{"database": database, "table": table}).Exec(cttx); err != nil {
		return nil, fmt.Errorf("could not delete existing snapshots: %w", err)
	}

	if result, err = s.iceberg.ListSnapshots(cttx, database, table); err != nil {
		return nil, fmt.Errorf("could not list snapshots: %w", err)
	}

	snapshots := make([]Snapshot, len(result))
	for i := range result {
		snapshots[i].Database = database
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
		insert := cttx.Q().Into("snapshots").Replace().Records(chunk)

		if _, err = insert.Exec(cttx); err != nil {
			return nil, fmt.Errorf("could not save snapshots: %w", err)
		}
	}

	s.logger.Info(cttx, "refreshed %d snapshots for table %s.%s", len(snapshots), database, table)

	return snapshots, nil
}

func (s *ServiceRefresh) RefreshFull(cttx sqlc.Tx) ([]CatalogTable, error) {
	var err error
	var databases []CatalogDatabase
	var tables []CatalogTable

	if databases, err = s.iceberg.ListDatabases(cttx); err != nil {
		return nil, fmt.Errorf("could not list databases: %w", err)
	}

	var allTables []CatalogTable
	for _, database := range databases {
		if tables, err = s.RefreshDatabase(cttx, database.Name); err != nil {
			return nil, fmt.Errorf("could not refresh database %s: %w", database.Name, err)
		}

		allTables = append(allTables, tables...)
	}

	return allTables, nil
}

func (s *ServiceRefresh) RefreshDatabase(cttx sqlc.Tx, database string) ([]CatalogTable, error) {
	var err error
	var icebergTables, storedTables []CatalogTable

	if icebergTables, err = s.iceberg.ListTables(cttx, database); err != nil {
		return nil, fmt.Errorf("could not list tables for database %s: %w", database, err)
	}

	if storedTables, err = s.listStoredTables(cttx, database); err != nil {
		return nil, fmt.Errorf("could not list stored tables for database %s: %w", database, err)
	}

	_, staleTables := funk.Difference(icebergTables, storedTables)
	for _, table := range staleTables {
		if err = s.deleteStaleTable(cttx, table.Database, table.Name); err != nil {
			return nil, fmt.Errorf("could not delete stale table %s.%s: %w", table.Database, table.Name, err)
		}
	}

	s.logger.Info(cttx, "deleted %d stale tables from database %s", len(staleTables), database)
	s.logger.Info(cttx, "starting database refresh for %d tables in %s", len(icebergTables), database)

	for _, table := range icebergTables {
		if err = s.RefreshTableFull(cttx, table.Database, table.Name); err != nil {
			return nil, fmt.Errorf("could not refresh table %s.%s: %w", table.Database, table.Name, err)
		}
	}

	return icebergTables, nil
}

func (s *ServiceRefresh) RefreshTableFull(cttx sqlc.Tx, database string, table string) error {
	var err error

	s.logger.Info(cttx, "refreshing table %s.%s", database, table)

	if _, err = s.RefreshTable(cttx, database, table); err != nil {
		return fmt.Errorf("could not refresh table %s.%s: %w", database, table, err)
	}

	if _, err = s.RefreshPartitions(cttx, database, table); err != nil {
		return fmt.Errorf("could not refresh partitions for table %s.%s: %w", database, table, err)
	}

	if _, err = s.RefreshSnapshots(cttx, database, table); err != nil {
		return fmt.Errorf("could not refresh snapshots for table %s.%s: %w", database, table, err)
	}

	return nil
}

func (s *ServiceRefresh) listStoredTables(cttx sqlc.Tx, database string) ([]CatalogTable, error) {
	type tableRow struct {
		Database string `db:"database"`
		Name     string `db:"name"`
	}

	rows := make([]tableRow, 0)
	q := cttx.Q().From("tables").Column(sqlc.Col("database")).Column(sqlc.Col("name"))
	if database != "" {
		q = q.Where(sqlc.Eq{"database": database})
	}

	if err := q.Select(cttx, &rows); err != nil {
		return nil, fmt.Errorf("could not query stored tables: %w", err)
	}

	tables := make([]CatalogTable, len(rows))
	for i, row := range rows {
		tables[i] = CatalogTable(row)
	}

	return tables, nil
}

func (s *ServiceRefresh) deleteStaleTable(cttx sqlc.Tx, database string, name string) error {
	cleanupSteps := map[string]string{
		"partitions": "table",
		"snapshots":  "table",
		"tasks":      "table",
		"tables":     "name",
	}

	for table, column := range cleanupSteps {
		where := sqlc.Eq{column: name, "database": database}

		if _, err := cttx.Q().Delete(table).Where(where).Exec(cttx); err != nil {
			return fmt.Errorf("could not delete from %s: %w", table, err)
		}
	}

	s.logger.Info(cttx, "deleted stale table %s.%s from metadata store", database, name)

	return nil
}
