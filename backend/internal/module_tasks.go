package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/coffin"
	"github.com/justtrackio/gosoline/pkg/kernel"
	"github.com/justtrackio/gosoline/pkg/log"
)

type TaskSettings struct {
	Enabled      bool          `cfg:"enabled"`
	PollInterval time.Duration `cfg:"poll_interval" default:"1s"`
}

type moduleTasksCtxKey struct{}

func ProvideModuleTasks(ctx context.Context, config cfg.Config, logger log.Logger) (*ModuleTasks, error) {
	return appctx.Provide(ctx, moduleTasksCtxKey{}, func() (*ModuleTasks, error) {
		return NewModuleTasks(ctx, config, logger)
	})
}

func NewModuleTasks(ctx context.Context, config cfg.Config, logger log.Logger) (*ModuleTasks, error) {
	var err error
	var serviceTaskQueue *ServiceTaskQueue
	var serviceMaintenanceExecutor *ServiceMaintenanceExecutor

	if serviceTaskQueue, err = NewServiceTaskQueue(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create task queue service: %w", err)
	}

	if serviceMaintenanceExecutor, err = NewServiceMaintenanceExecutor(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create maintenance executor service: %w", err)
	}

	settings := &TaskSettings{}
	if err = config.UnmarshalKey("tasks", settings); err != nil {
		return nil, fmt.Errorf("could not unmarshal tasks settings: %w", err)
	}

	module := &ModuleTasks{
		logger:                     logger.WithChannel("task_worker"),
		serviceTaskQueue:           serviceTaskQueue,
		serviceMaintenanceExecutor: serviceMaintenanceExecutor,
		settings:                   settings,
	}

	return module, nil
}

type ModuleTasks struct {
	kernel.ServiceStage
	kernel.BackgroundModule

	logger                     log.Logger
	serviceTaskQueue           TaskClaimer
	serviceMaintenanceExecutor interface {
		All() []MaintenanceExecutor
		ForEngine(TaskEngine) (MaintenanceExecutor, error)
	}
	settings *TaskSettings
}

func (m *ModuleTasks) Run(ctx context.Context) error {
	if !m.settings.Enabled {
		return nil
	}

	m.logger.Info(ctx, "starting task worker with db-backed concurrency control")

	if err := m.failOrphanedRunningTasks(ctx); err != nil {
		return err
	}

	for _, executor := range m.serviceMaintenanceExecutor.All() {
		if err := executor.Reconcile(ctx); err != nil {
			return fmt.Errorf("could not reconcile running %s tasks: %w", executor.Engine(), err)
		}
	}

	ticker := time.NewTicker(m.settings.PollInterval)
	defer ticker.Stop()

	cfn, ctx := coffin.WithContext(ctx)
	for _, executor := range m.serviceMaintenanceExecutor.All() {
		cfn.GoWithContext(ctx, func(ctx context.Context) error {
			m.logger.Info(ctx, "starting maintenance runner for engine %s", executor.Engine())

			return executor.Run(ctx)
		})
	}

	cfn.GoWithContext(ctx, func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				m.tryProcessTasks(ctx, cfn)
			}
		}
	})

	return cfn.Wait()
}

func (m *ModuleTasks) failOrphanedRunningTasks(ctx context.Context) error {
	var err error
	var failed int64

	engines := make([]TaskEngine, 0)
	for _, executor := range m.serviceMaintenanceExecutor.All() {
		engines = append(engines, executor.Engine())
	}

	if failed, err = m.serviceTaskQueue.FailOrphanedRunningTasks(ctx, engines); err != nil {
		return fmt.Errorf("could not fail orphaned running tasks: %w", err)
	}

	if failed > 0 {
		m.logger.Warn(ctx, "failed %d running tasks of engines other than %v", failed, engines)
	}

	return nil
}

func (m *ModuleTasks) tryProcessTasks(ctx context.Context, cfn coffin.Coffin) {
	var err error
	var task *Task

	for {
		if task, err = m.serviceTaskQueue.ClaimTask(ctx); err != nil {
			m.logger.Error(ctx, "failed to claim task: %s", err)

			return
		}

		if task == nil {
			return
		}

		m.logger.Info(ctx, "picked up task %d (%s for %s.%s)", task.Id, task.Kind, task.Database, task.Table)
		claimedTask := task
		cfn.GoWithContext(ctx, func(ctx context.Context) error {
			if err := m.processTask(ctx, claimedTask); err != nil {
				m.logger.Error(ctx, "failed to process task %d: %s", claimedTask.Id, err)
			}

			return nil
		})
	}
}

func (m *ModuleTasks) processTask(ctx context.Context, task *Task) error {
	var err error
	var executor MaintenanceExecutor

	if executor, err = m.serviceMaintenanceExecutor.ForEngine(TaskEngine(task.Engine)); err != nil {
		if completeErr := m.serviceTaskQueue.CompleteTask(ctx, task.Id, nil, err); completeErr != nil {
			return fmt.Errorf("could not complete task %d after engine lookup failure: %w", task.Id, completeErr)
		}

		return nil
	}

	if err = executor.ProcessTask(ctx, task); err != nil {
		wrappedErr := fmt.Errorf("engine %s failed to process task %d: %w", executor.Engine(), task.Id, err)
		if completeErr := m.serviceTaskQueue.CompleteTask(ctx, task.Id, nil, wrappedErr); completeErr != nil {
			return fmt.Errorf("could not complete task %d after processing failure: %w", task.Id, completeErr)
		}

		return nil
	}

	return nil
}
