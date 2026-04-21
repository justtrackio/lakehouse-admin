package internal

import (
	"fmt"

	"github.com/justtrackio/gosoline/pkg/cfg"
)

type MaintenanceScheduleSettings struct {
	Enabled           bool                                 `cfg:"enabled"`
	Cron              string                               `cfg:"cron"`
	Optimize          MaintenanceScheduleOptimizeSettings  `cfg:"optimize"`
	ExpireSnapshots   MaintenanceScheduleRetentionSettings `cfg:"expire_snapshots"`
	RemoveOrphanFiles MaintenanceScheduleRetentionSettings `cfg:"remove_orphan_files"`
}

type MaintenanceScheduleOptimizeSettings struct {
	LookbackDays     int    `cfg:"lookback_days" default:"30"`
	TargetFileSizeMb int    `cfg:"target_file_size_mb" default:"512"`
	ChunkBy          string `cfg:"chunk_by" default:"daily"`
}

type MaintenanceScheduleRetentionSettings struct {
	RetentionDays int `cfg:"retention_days" default:"7"`
}

func ReadMaintenanceScheduleSettings(config cfg.Config) (*MaintenanceScheduleSettings, error) {
	settings := &MaintenanceScheduleSettings{}
	if err := config.UnmarshalKey("tasks.schedule", settings); err != nil {
		return nil, fmt.Errorf("could not unmarshal maintenance schedule settings: %w", err)
	}

	if _, err := parseStandardCronSchedule(settings.Cron); err != nil {
		return nil, fmt.Errorf("invalid maintenance schedule cron expression: %w", err)
	}

	if settings.Optimize.LookbackDays < 1 {
		return nil, fmt.Errorf("tasks.schedule.optimize.lookback_days must be at least 1")
	}

	if settings.Optimize.TargetFileSizeMb < 1 {
		return nil, fmt.Errorf("tasks.schedule.optimize.target_file_size_mb must be at least 1")
	}

	if _, err := normalizeOptimizeChunkBy(settings.Optimize.ChunkBy); err != nil {
		return nil, fmt.Errorf("invalid optimize chunking in maintenance schedule: %w", err)
	}

	if settings.ExpireSnapshots.RetentionDays < 1 {
		return nil, fmt.Errorf("tasks.schedule.expire_snapshots.retention_days must be at least 1")
	}

	if settings.RemoveOrphanFiles.RetentionDays < 1 {
		return nil, fmt.Errorf("tasks.schedule.remove_orphan_files.retention_days must be at least 1")
	}

	return settings, nil
}
