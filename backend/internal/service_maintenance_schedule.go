package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type MaintenanceScheduleCycleResult struct {
	TableCount                    int
	OptimizeTaskCount             int
	OptimizeFailureCount          int
	ExpireSnapshotsTaskCount      int
	ExpireSnapshotsFailureCount   int
	RemoveOrphanFilesTaskCount    int
	RemoveOrphanFilesFailureCount int
}

type ServiceMaintenanceSchedule struct {
	logger   log.Logger
	metadata *ServiceMetadata
	tasks    *ServiceTasks
	settings *MaintenanceScheduleSettings
}

func NewServiceMaintenanceSchedule(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceMaintenanceSchedule, error) {
	var err error
	var metadata *ServiceMetadata
	var tasks *ServiceTasks
	var settings *MaintenanceScheduleSettings

	if metadata, err = NewServiceMetadata(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create metadata service: %w", err)
	}

	if tasks, err = NewServiceTasks(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create tasks service: %w", err)
	}

	if settings, err = ReadMaintenanceScheduleSettings(config); err != nil {
		return nil, fmt.Errorf("could not read maintenance schedule settings: %w", err)
	}

	return &ServiceMaintenanceSchedule{
		logger:   logger.WithChannel("maintenance_schedule"),
		metadata: metadata,
		tasks:    tasks,
		settings: settings,
	}, nil
}

func (s *ServiceMaintenanceSchedule) RunCycle(ctx context.Context, now time.Time) (*MaintenanceScheduleCycleResult, error) {
	var err error
	var tables []TableDescription
	var taskIDs []int64

	if tables, err = s.metadata.ListAllTables(ctx); err != nil {
		return nil, fmt.Errorf("could not list tables for maintenance scheduling: %w", err)
	}

	result := &MaintenanceScheduleCycleResult{TableCount: len(tables)}
	if len(tables) == 0 {
		return result, nil
	}

	from, to := scheduledOptimizeRange(now.UTC(), s.settings.Optimize.LookbackDays)
	for _, table := range tables {
		if taskIDs, err = s.tasks.EnqueueOptimize(ctx, table.Database, table.Name, s.settings.Optimize.TargetFileSizeMb, from, to, s.settings.Optimize.ChunkBy); err != nil {
			result.OptimizeFailureCount++
			s.logger.Warn(ctx, "failed to enqueue scheduled optimize for table %s.%s: %s", table.Database, table.Name, err)

			continue
		}

		result.OptimizeTaskCount += len(taskIDs)
	}

	for _, table := range tables {
		if _, err = s.tasks.EnqueueExpireSnapshots(ctx, table.Database, table.Name, s.settings.ExpireSnapshots.RetentionDays); err != nil {
			result.ExpireSnapshotsFailureCount++
			s.logger.Warn(ctx, "failed to enqueue scheduled expire_snapshots for table %s.%s: %s", table.Database, table.Name, err)

			continue
		}

		result.ExpireSnapshotsTaskCount++
	}

	for _, table := range tables {
		if _, err = s.tasks.EnqueueRemoveOrphanFiles(ctx, table.Database, table.Name, s.settings.RemoveOrphanFiles.RetentionDays); err != nil {
			result.RemoveOrphanFilesFailureCount++
			s.logger.Warn(ctx, "failed to enqueue scheduled remove_orphan_files for table %s.%s: %s", table.Database, table.Name, err)

			continue
		}

		result.RemoveOrphanFilesTaskCount++
	}

	return result, nil
}

func scheduledOptimizeRange(now time.Time, lookbackDays int) (from time.Time, to time.Time) {
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	return day.AddDate(0, 0, -(lookbackDays - 1)), day
}
