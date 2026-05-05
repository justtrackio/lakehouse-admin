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
		return time.Time{}, fmt.Errorf("could not get table description for table %s: %w", name, err)
	}

	return table.UpdatedAt, nil
}

func (s *ServiceRefresh) ListTables(ctx context.Context, database string) ([]CatalogTable, error) {
	return s.iceberg.ListTables(ctx, database)
}

func (s *ServiceRefresh) RefreshAllTables(cttx sqlc.Tx) ([]CatalogTable, error) {
	var err error
	var tables []CatalogTable

	if tables, err = s.reconcileTableInventory(cttx); err != nil {
		return nil, fmt.Errorf("could not reconcile table inventory: %w", err)
	}

	for _, table := range tables {
		if _, err = s.RefreshTable(cttx, table.Database, table.Name); err != nil {
			return nil, fmt.Errorf("could not refresh table %s.%s: %w", table.Database, table.Name, err)
		}
	}

	return tables, nil
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
	tables, err := s.reconcileTableInventory(cttx)
	if err != nil {
		return nil, fmt.Errorf("could not reconcile table inventory: %w", err)
	}

	s.logger.Info(cttx, "starting full refresh for %d tables", len(tables))

	for _, table := range tables {
		if err = s.RefreshTableFull(cttx, table.Database, table.Name); err != nil {
			return nil, fmt.Errorf("could not refresh table %s.%s: %w", table.Database, table.Name, err)
		}
	}

	return tables, nil
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


func (s *ServiceRefresh) reconcileTableInventory(cttx sqlc.Tx) ([]CatalogTable, error) {
	var err error
	var databases []CatalogDatabase
	var icebergTables, databaseTables, staleTables []CatalogTable

	if databases, err = s.iceberg.ListDatabases(cttx); err != nil {
		return nil, fmt.Errorf("could not list databases: %w", err)
	}

	icebergTables = make([]CatalogTable, 0)
	for _, database := range databases {
		var tables []CatalogTable
		if tables, err = s.iceberg.ListTables(cttx, database.Name); err != nil {
			return nil, fmt.Errorf("could not list tables for database %s: %w", database.Name, err)
		}

		icebergTables = append(icebergTables, tables...)
	}

	if databaseTables, err = s.listStoredTables(cttx); err != nil {
		return nil, fmt.Errorf("could not list stored tables: %w", err)
	}

	_, staleTables = funk.Difference(icebergTables, databaseTables)

	for _, table := range staleTables {
		if err = s.deleteStaleTable(cttx, table.Database, table.Name); err != nil {
			return nil, fmt.Errorf("could not delete stale table %s.%s: %w", table.Database, table.Name, err)
		}
	}

	s.logger.Info(cttx, "deleted %d stale tables from metadata store", len(staleTables))

	return icebergTables, nil
}

func (s *ServiceRefresh) listStoredTables(cttx sqlc.Tx) ([]CatalogTable, error) {
	type tableRow struct {
		Database string `db:"database"`
		Name string `db:"name"`
	}

	rows := make([]tableRow, 0)
	if err := cttx.Q().From("tables").Column(sqlc.Col("database")).Column(sqlc.Col("name")).Select(cttx, &rows); err != nil {
		return nil, fmt.Errorf("could not query stored tables: %w", err)
	}

	tables := make([]CatalogTable, len(rows))
	for i, row := range rows {
		tables[i] = CatalogTable{Database: row.Database, Name: row.Name}
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
		where := sqlc.Eq{column: name}
		if table == "tables" {
			where["database"] = database
		} else {
			where["database"] = database
		}

		if _, err := cttx.Q().Delete(table).Where(where).Exec(cttx); err != nil {
			return fmt.Errorf("could not delete from %s: %w", table, err)
		}
	}

	s.logger.Info(cttx, "deleted stale table %s.%s from metadata store", database, name)

	return nil
}
