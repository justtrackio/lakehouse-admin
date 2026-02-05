package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/kernel"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewModuleMaintenance(ctx context.Context, config cfg.Config, logger log.Logger) (kernel.Module, error) {
	var err error
	var serviceMaintenanceExecutor *ServiceMaintenanceExecutor
	var serviceMaintenanceTaskQueue *ServiceMaintenance
	var serviceRefresh *ServiceRefresh
	var sqlClient sqlc.Client

	if serviceMaintenanceExecutor, err = NewServiceMaintenanceExecutor(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create maintenance executor service: %w", err)
	}

	if serviceMaintenanceTaskQueue, err = NewServiceMaintenance(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create maintenance task queue service: %w", err)
	}

	if serviceRefresh, err = NewServiceRefresh(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create refresh service: %w", err)
	}

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}

	workerCount, _ := config.GetInt("maintenance.worker_count")
	if workerCount < 1 {
		workerCount = 1
	}

	pollInterval, _ := config.GetDuration("maintenance.poll_interval")
	if pollInterval == 0 {
		pollInterval = time.Second
	}

	return &ModuleMaintenance{
		logger:                      logger.WithChannel("maintenance_worker"),
		serviceMaintenanceExecutor:  serviceMaintenanceExecutor,
		serviceMaintenanceTaskQueue: serviceMaintenanceTaskQueue,
		serviceRefresh:              serviceRefresh,
		sqlClient:                   sqlClient,
		workerCount:                 workerCount,
		pollInterval:                pollInterval,
	}, nil
}

type ModuleMaintenance struct {
	logger                      log.Logger
	serviceMaintenanceExecutor  *ServiceMaintenanceExecutor
	serviceMaintenanceTaskQueue *ServiceMaintenance
	serviceRefresh              *ServiceRefresh
	sqlClient                   sqlc.Client
	workerCount                 int
	pollInterval                time.Duration
}

func (m *ModuleMaintenance) Run(ctx context.Context) error {
	m.logger.Info(ctx, "starting maintenance worker pool with %d workers", m.workerCount)

	for i := 0; i < m.workerCount; i++ {
		go m.workerLoop(ctx, i)
	}

	<-ctx.Done()
	return nil
}

func (m *ModuleMaintenance) workerLoop(ctx context.Context, workerId int) {
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Try to claim a task
			task, err := m.serviceMaintenanceTaskQueue.ClaimTask(ctx)
			if err != nil {
				m.logger.Error(ctx, "worker %d: failed to claim task: %s", workerId, err)
				continue
			}
			if task == nil {
				continue // No task
			}

			m.logger.Info(ctx, "worker %d: picked up task %d (%s for %s)", workerId, task.Id, task.Kind, task.Table)
			m.processTask(ctx, task)
		}
	}
}

func (m *ModuleMaintenance) processTask(ctx context.Context, task *MaintenanceTask) {
	var result map[string]any
	var err error

	input := task.Input.Get()

	switch task.Kind {
	case "expire_snapshots":
		result, err = m.processExpireSnapshots(ctx, task.Table, input)
	case "remove_orphan_files":
		result, err = m.processRemoveOrphanFiles(ctx, task.Table, input)
	case "optimize":
		result, err = m.processOptimize(ctx, task.Table, input)
	default:
		err = fmt.Errorf("unknown task kind: %s", task.Kind)
	}

	if completeErr := m.serviceMaintenanceTaskQueue.CompleteTask(ctx, task.Id, result, err); completeErr != nil {
		m.logger.Error(ctx, "failed to complete task %d: %s", task.Id, completeErr)
	} else {
		status := "success"
		if err != nil {
			status = "error"
		}
		m.logger.Info(ctx, "task %d finished with status: %s", task.Id, status)
	}
}

func (m *ModuleMaintenance) processExpireSnapshots(ctx context.Context, table string, input map[string]any) (map[string]any, error) {
	retentionDays, _ := input["retention_days"].(float64)
	retainLast, _ := input["retain_last"].(float64)

	res, err := m.serviceMaintenanceExecutor.ExecuteExpireSnapshots(ctx, table, int(retentionDays), int(retainLast))
	if err != nil {
		return nil, err
	}

	// Follow-up: Refresh Snapshots
	err = m.sqlClient.WithTx(ctx, func(cttx sqlc.Tx) error {
		_, err := m.serviceRefresh.RefreshSnapshots(cttx, table)
		return err
	})

	if err != nil {
		m.logger.Warn(ctx, "failed to refresh snapshots after expiring for table %s: %s", table, err)
		// We don't fail the task if refresh fails, but it's good to know
	}

	return map[string]any{
		"table":                  res.Table,
		"retention_days":         res.RetentionDays,
		"retain_last":            res.RetainLast,
		"clean_expired_metadata": res.CleanExpiredMetadata,
		"status":                 res.Status,
	}, nil
}

func (m *ModuleMaintenance) processRemoveOrphanFiles(ctx context.Context, table string, input map[string]any) (map[string]any, error) {
	retentionDays, _ := input["retention_days"].(float64)

	res, err := m.serviceMaintenanceExecutor.ExecuteRemoveOrphanFiles(ctx, table, int(retentionDays))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"table":          res.Table,
		"retention_days": res.RetentionDays,
		"metrics":        res.Metrics,
		"status":         res.Status,
	}, nil
}

func (m *ModuleMaintenance) processOptimize(ctx context.Context, table string, input map[string]any) (map[string]any, error) {
	fileSizeThresholdMb, _ := input["file_size_threshold_mb"].(float64)

	var from, to DateTime

	// Placeholder:
	fVal := input["from"]
	tVal := input["to"]

	from = parseDateTimeAny(fVal)
	to = parseDateTimeAny(tVal)

	res, err := m.serviceMaintenanceExecutor.ExecuteOptimize(ctx, table, int(fileSizeThresholdMb), from, to)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"table":                  res.Table,
		"file_size_threshold_mb": res.FileSizeThresholdMb,
		"where":                  res.Where,
		"status":                 res.Status,
	}, nil
}

func parseDateTimeAny(v any) DateTime {
	// Best effort parser
	if s, ok := v.(string); ok {
		t, _ := time.Parse(time.RFC3339, s)
		return DateTime{Time: t}
	}
	// Check if it's a map (if struct serialization)
	if m, ok := v.(map[string]any); ok {
		// Assuming standard struct serialization?
		// or if it's stored as `time.Time` string?
		// Let's try to find a time field
		if tStr, ok := m["Time"].(string); ok {
			t, _ := time.Parse(time.RFC3339, tStr)
			return DateTime{Time: t}
		}
	}
	return DateTime{Time: time.Now()} // Fallback
}
