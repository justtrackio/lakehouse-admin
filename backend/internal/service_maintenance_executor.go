package internal

import (
	"context"
	"fmt"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewServiceMaintenanceExecutor(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceMaintenanceExecutor, error) {
	var err error
	var trino *TrinoMaintenanceExecutor
	var spark *SparkMaintenanceExecutor

	if trino, err = NewTrinoMaintenanceExecutor(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create trino maintenance executor: %w", err)
	}

	if spark, err = NewSparkMaintenanceExecutor(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create spark maintenance executor: %w", err)
	}

	return &ServiceMaintenanceExecutor{
		all: []MaintenanceExecutor{trino, spark},
		executors: map[TaskEngine]MaintenanceExecutor{
			TaskEngineTrino: trino,
			TaskEngineSpark: spark,
		},
	}, nil
}

type ServiceMaintenanceExecutor struct {
	all       []MaintenanceExecutor
	executors map[TaskEngine]MaintenanceExecutor
}

func (s *ServiceMaintenanceExecutor) All() []MaintenanceExecutor {
	return s.all
}

func (s *ServiceMaintenanceExecutor) ForEngine(engine TaskEngine) (MaintenanceExecutor, error) {
	executor, ok := s.executors[engine]
	if !ok {
		return nil, fmt.Errorf("unsupported task engine %s", engine)
	}

	return executor, nil
}
