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

func (m *ModuleTasks) tryProcessTasks(ctx context.Context, cfn coffin.Coffin) {
	for {
		task, err := m.serviceTaskQueue.ClaimTask(ctx)
		if err != nil {
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
	executor, err := m.serviceMaintenanceExecutor.ForEngine(TaskEngine(task.Engine))
	if err != nil {
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
