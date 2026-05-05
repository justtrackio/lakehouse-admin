package internal

import (
	"context"
	"fmt"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewServiceIcebergAdmin(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceIcebergAdmin, error) {
	trino, err := ProvideTrinoClient(ctx, config, logger)
	if err != nil {
		return nil, fmt.Errorf("could not create trino client: %w", err)
	}

	return &ServiceIcebergAdmin{
		logger: logger.WithChannel("iceberg_admin"),
		trino:  trino,
	}, nil
}

type ServiceIcebergAdmin struct {
	logger log.Logger
	trino  *TrinoClient
}

func (s *ServiceIcebergAdmin) RollbackToSnapshot(ctx context.Context, database string, logicalName string, snapshotID int64) error {
	qualifiedTable := qualifiedTableName("lakehouse", database, logicalName)
	query := fmt.Sprintf("ALTER TABLE %s EXECUTE rollback_to_snapshot(%d)", qualifiedTable, snapshotID)

	if err := s.trino.Exec(ctx, query); err != nil {
		return fmt.Errorf("could not rollback table %s to snapshot %d: %w", logicalName, snapshotID, err)
	}

	s.logger.Info(ctx, "rolled back table %s to snapshot %d", logicalName, snapshotID)

	return nil
}
