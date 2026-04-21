package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/kernel"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewModuleMaintenanceSchedule(ctx context.Context, config cfg.Config, logger log.Logger) (kernel.Module, error) {
	var err error
	var service *ServiceMaintenanceSchedule
	var settings *MaintenanceScheduleSettings

	if service, err = NewServiceMaintenanceSchedule(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create maintenance schedule service: %w", err)
	}

	if settings, err = ReadMaintenanceScheduleSettings(config); err != nil {
		return nil, fmt.Errorf("could not read maintenance schedule settings: %w", err)
	}

	return &ModuleMaintenanceSchedule{
		logger:   logger.WithChannel("maintenance_schedule"),
		service:  service,
		settings: settings,
	}, nil
}

type ModuleMaintenanceSchedule struct {
	kernel.ServiceStage
	kernel.BackgroundModule

	logger   log.Logger
	service  *ServiceMaintenanceSchedule
	settings *MaintenanceScheduleSettings
}

func (m *ModuleMaintenanceSchedule) Run(ctx context.Context) error {
	if !m.settings.Enabled {
		return nil
	}

	ticker := time.NewTicker(m.settings.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			m.runMaintenanceCycle(ctx)
		}
	}
}

func (m *ModuleMaintenanceSchedule) runMaintenanceCycle(ctx context.Context) {
	m.logger.Info(ctx, "starting scheduled maintenance cycle")

	var err error
	var result *MaintenanceScheduleCycleResult


	if result, err = m.service.RunCycle(ctx, time.Now().UTC()); err != nil {
		m.logger.Error(ctx, "failed scheduled maintenance cycle: %s", err)

		return
	}

	m.logger.Info(
		ctx,
		"finished scheduled maintenance cycle for %d tables (optimize: %d tasks, %d failures; expire_snapshots: %d tasks, %d failures; remove_orphan_files: %d tasks, %d failures)",
		result.TableCount,
		result.OptimizeTaskCount,
		result.OptimizeFailureCount,
		result.ExpireSnapshotsTaskCount,
		result.ExpireSnapshotsFailureCount,
		result.RemoveOrphanFilesTaskCount,
		result.RemoveOrphanFilesFailureCount,
	)
}
