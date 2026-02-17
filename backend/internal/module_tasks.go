package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/coffin"
	"github.com/justtrackio/gosoline/pkg/kernel"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/marusama/semaphore/v2"
	"github.com/spf13/cast"
)

func NewModuleTasks(ctx context.Context, config cfg.Config, logger log.Logger) (kernel.Module, error) {
	return ProvideModuleTasks(ctx, config, logger)
}

func ProvideModuleTasks(ctx context.Context, config cfg.Config, logger log.Logger) (*ModuleTasks, error) {
	var err error
	var serviceTaskQueue *ServiceTaskQueue
	var serviceMaintenanceExecutor *ServiceMaintenanceExecutor
	var serviceRefresh *ServiceRefresh
	var serviceSettings *ServiceSettings
	var sqlClient sqlc.Client

	if serviceTaskQueue, err = NewServiceTaskQueue(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create task queue service: %w", err)
	}

	if serviceMaintenanceExecutor, err = NewServiceMaintenanceExecutor(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create maintenance executor service: %w", err)
	}

	if serviceRefresh, err = NewServiceRefresh(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create refresh service: %w", err)
	}

	if serviceSettings, err = NewServiceSettings(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create settings service: %w", err)
	}

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}

	// Get default from config
	defaultWorkerCount, err := config.GetInt("tasks.worker_count")
	if err != nil {
		defaultWorkerCount = 1
	}

	if defaultWorkerCount < 1 {
		defaultWorkerCount = 1
	}

	// Try to load from database, fall back to config default
	workerCount, err := serviceSettings.GetIntSetting(ctx, "task_concurrency", defaultWorkerCount)
	if err != nil {
		logger.Warn(ctx, "could not load task concurrency from settings, using default: %s", err)
		workerCount = defaultWorkerCount
	}

	pollInterval, err := config.GetDuration("tasks.poll_interval")
	if err != nil || pollInterval == 0 {
		pollInterval = time.Second
	}

	module := &ModuleTasks{
		logger:                     logger.WithChannel("task_worker"),
		serviceTaskQueue:           serviceTaskQueue,
		serviceMaintenanceExecutor: serviceMaintenanceExecutor,
		serviceRefresh:             serviceRefresh,
		sqlClient:                  sqlClient,
		pollInterval:               pollInterval,
		sem:                        semaphore.New(workerCount),
	}

	return module, nil
}

type ModuleTasks struct {
	logger                     log.Logger
	serviceTaskQueue           TaskClaimer
	serviceMaintenanceExecutor MaintenanceExecutor
	serviceRefresh             SnapshotRefresher
	sqlClient                  sqlc.Client
	pollInterval               time.Duration
	sem                        semaphore.Semaphore
}

func (m *ModuleTasks) Run(ctx context.Context) error {
	m.logger.Info(ctx, "starting task worker pool with %d workers", m.sem.GetLimit())

	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	cfn, ctx := coffin.WithContext(ctx)
	cfn.GoWithContext(ctx, func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				m.tryProcessTask(ctx, cfn)
			}
		}
	})

	return cfn.Wait()
}

func (m *ModuleTasks) tryProcessTask(ctx context.Context, cfn coffin.Coffin) {
	if ok := m.sem.TryAcquire(1); !ok {
		return
	}

	// Try to claim a task
	task, err := m.serviceTaskQueue.ClaimTask(ctx)
	if err != nil {
		m.sem.Release(1)
		m.logger.Error(ctx, "failed to claim task: %s", err)

		return
	}

	if task == nil {
		m.sem.Release(1)

		return // No task
	}

	m.logger.Info(ctx, "picked up task %d (%s for %s)", task.Id, task.Kind, task.Table)
	cfn.GoWithContext(ctx, func(ctx context.Context) error {
		defer m.sem.Release(1)

		if err := m.processTask(ctx, task); err != nil {
			m.logger.Error(ctx, "failed to process task %d: %s", task.Id, err)
		}

		return nil
	})
}

func (m *ModuleTasks) processTask(ctx context.Context, task *Task) error {
	var err error
	var result map[string]any

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

	if completeErr := m.serviceTaskQueue.CompleteTask(ctx, task.Id, result, err); completeErr != nil {
		m.logger.Error(ctx, "failed to complete task %d: %s", task.Id, completeErr)
	} else {
		status := "success"
		if err != nil {
			status = "error"
		}
		m.logger.Info(ctx, "task %d finished with status: %s", task.Id, status)
	}

	return nil
}

func (m *ModuleTasks) processExpireSnapshots(ctx context.Context, table string, input map[string]any) (map[string]any, error) {
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

func (m *ModuleTasks) processRemoveOrphanFiles(ctx context.Context, table string, input map[string]any) (map[string]any, error) {
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

func (m *ModuleTasks) processOptimize(ctx context.Context, table string, input map[string]any) (map[string]any, error) {
	fileSizeThresholdMb, _ := input["file_size_threshold_mb"].(float64)

	from := cast.ToTime(input["from"])
	to := cast.ToTime(input["to"])

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

// SetWorkerCount dynamically adjusts the number of workers in the pool
func (m *ModuleTasks) SetWorkerCount(newCount int) {
	if newCount < 1 {
		newCount = 1
	}

	m.sem.SetLimit(newCount)
}

// GetWorkerCount returns the current worker count limit.
func (m *ModuleTasks) GetWorkerCount() int {
	return m.sem.GetLimit()
}
