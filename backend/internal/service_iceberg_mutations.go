package internal

import (
	"context"
	"fmt"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewServiceIcebergAdmin(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceIcebergAdmin, error) {
	var err error
	var trino *TrinoClient
	var settings *IcebergSettings

	if trino, err = ProvideTrinoClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create trino client: %w", err)
	}

	if settings, err = ReadIcebergSettings(config); err != nil {
		return nil, fmt.Errorf("could not read iceberg settings: %w", err)
	}

	return &ServiceIcebergAdmin{
		logger:   logger.WithChannel("iceberg_admin"),
		trino:    trino,
		settings: settings,
	}, nil
}

type ServiceIcebergAdmin struct {
	logger   log.Logger
	trino    *TrinoClient
	settings *IcebergSettings
}

func (s *ServiceIcebergAdmin) RollbackToSnapshot(ctx context.Context, database string, logicalName string, snapshotID int64) error {
	qualifiedTable := qualifiedTableName(s.settings.Catalog, database, logicalName)
	query := fmt.Sprintf("ALTER TABLE %s EXECUTE rollback_to_snapshot(%d)", qualifiedTable, snapshotID)

	if err := s.trino.Exec(ctx, query); err != nil {
		return fmt.Errorf("could not rollback table %s.%s to snapshot %d: %w", database, logicalName, snapshotID, err)
	}

	s.logger.Info(ctx, "rolled back table %s.%s to snapshot %d", database, logicalName, snapshotID)

	return nil
}
