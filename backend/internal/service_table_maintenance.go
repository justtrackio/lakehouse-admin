package internal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type TableMaintenanceSettings struct {
	OptimizeDisabled          bool `json:"optimize_disabled" db:"optimize_disabled"`
	ExpireSnapshotsDisabled   bool `json:"expire_snapshots_disabled" db:"expire_snapshots_disabled"`
	RemoveOrphanFilesDisabled bool `json:"remove_orphan_files_disabled" db:"remove_orphan_files_disabled"`
}

type ServiceTableMaintenance struct {
	sqlClient sqlc.Client
}

func NewServiceTableMaintenance(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceTableMaintenance, error) {
	var err error
	var sqlClient sqlc.Client

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sql client: %w", err)
	}

	return &ServiceTableMaintenance{sqlClient: sqlClient}, nil
}

func (s *ServiceTableMaintenance) Get(ctx context.Context, database string, table string) (*TableMaintenanceSettings, error) {
	settings := &TableMaintenanceSettings{}
	if err := s.sqlClient.Q().From("tables").Where(sqlc.Eq{"database": database, "name": table}).Get(ctx, settings); err != nil {
		return nil, fmt.Errorf("could not get maintenance settings for table %s.%s: %w", database, table, err)
	}

	return settings, nil
}

func (s *ServiceTableMaintenance) IsDisabled(ctx context.Context, database string, table string, kind string) (bool, error) {
	var err error
	var settings *TableMaintenanceSettings

	if settings, err = s.Get(ctx, database, table); err != nil {
		return false, err
	}

	return settings.isDisabled(kind), nil
}

func (s *ServiceTableMaintenance) IsDisabledInTx(ctx sqlc.Tx, database string, table string, kind string) (bool, error) {
	settings := &TableMaintenanceSettings{}
	if err := ctx.Q().From("tables").Where(sqlc.Eq{"database": database, "name": table}).Get(ctx, settings); err != nil {
		return false, fmt.Errorf("could not get maintenance settings for table %s.%s: %w", database, table, err)
	}

	return settings.isDisabled(kind), nil
}

func (s *ServiceTableMaintenance) Update(ctx context.Context, database string, table string, settings TableMaintenanceSettings) (int64, error) {
	var cancelled int64

	err := s.sqlClient.WithTx(ctx, func(cttx sqlc.Tx) error {
		var err error
		var res sqlc.Result
		var affected int64
		var count int64

		update := cttx.Q().Update("tables").
			Set("optimize_disabled", settings.OptimizeDisabled).
			Set("expire_snapshots_disabled", settings.ExpireSnapshotsDisabled).
			Set("remove_orphan_files_disabled", settings.RemoveOrphanFilesDisabled).
			Where(sqlc.Eq{"database": database, "name": table})
		if res, err = update.Exec(cttx); err != nil {
			return fmt.Errorf("could not update maintenance settings for table %s.%s: %w", database, table, err)
		}

		if affected, err = res.RowsAffected(); err != nil {
			return fmt.Errorf("could not get updated table count: %w", err)
		}

		if affected == 0 {
			return fmt.Errorf("could not update maintenance settings for table %s.%s: %w", database, table, sql.ErrNoRows)
		}

		for _, kind := range disabledTaskKinds(settings) {
			now := time.Now()
			result, err := cttx.Q().Update("tasks").
				Set("status", taskStatusCancelled).
				Set("finished_at", &now).
				Set("error_message", "cancelled because this maintenance task is disabled for the table").
				Where(sqlc.Eq{"database": database, "table": table, "kind": string(kind), "status": taskStatusQueued}).
				Exec(cttx)
			if err != nil {
				return fmt.Errorf("could not cancel queued %s tasks for table %s.%s: %w", kind, database, table, err)
			}

			if count, err = result.RowsAffected(); err != nil {
				return fmt.Errorf("could not get cancelled %s task count: %w", kind, err)
			}

			cancelled += count
		}

		return nil
	}, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, err
	}

	return cancelled, nil
}

func (s *TableMaintenanceSettings) isDisabled(kind string) bool {
	switch TaskKind(kind) {
	case TaskKindOptimize:
		return s.OptimizeDisabled
	case TaskKindExpireSnapshots:
		return s.ExpireSnapshotsDisabled
	case TaskKindRemoveOrphanFiles:
		return s.RemoveOrphanFilesDisabled
	default:
		return false
	}
}

func disabledTaskKinds(settings TableMaintenanceSettings) []TaskKind {
	kinds := make([]TaskKind, 0, 3)
	if settings.OptimizeDisabled {
		kinds = append(kinds, TaskKindOptimize)
	}
	if settings.ExpireSnapshotsDisabled {
		kinds = append(kinds, TaskKindExpireSnapshots)
	}
	if settings.RemoveOrphanFilesDisabled {
		kinds = append(kinds, TaskKindRemoveOrphanFiles)
	}

	return kinds
}
