package internal

import (
	"context"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/marusama/semaphore/v2"
)

// NewModuleTasksForTest creates a ModuleTasks for testing with injectable dependencies.
func NewModuleTasksForTest(
	logger log.Logger,
	taskClaimer TaskClaimer,
	executor MaintenanceExecutor,
	refresher SnapshotRefresher,
	sqlClient sqlc.Client,
	pollInterval time.Duration,
	sem semaphore.Semaphore,
) *ModuleTasks {
	return &ModuleTasks{
		logger:                     logger,
		serviceTaskQueue:           taskClaimer,
		serviceMaintenanceExecutor: executor,
		serviceRefresh:             refresher,
		sqlClient:                  sqlClient,
		pollInterval:               pollInterval,
		sem:                        sem,
	}
}

// ProcessTask exposes the unexported processTask method for testing.
func (m *ModuleTasks) ProcessTask(ctx context.Context, task *Task) error {
	return m.processTask(ctx, task)
}

// NewServiceTaskQueueForTest creates a ServiceTaskQueue for testing with injectable dependencies.
func NewServiceTaskQueueForTest(
	logger log.Logger,
	sqlClient sqlc.Client,
) *ServiceTaskQueue {
	return &ServiceTaskQueue{
		logger:    logger,
		sqlClient: sqlClient,
	}
}
