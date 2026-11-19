package internal

import (
	"fmt"

	"github.com/justtrackio/gosoline/pkg/cfg"
)

type TaskEngineSettings struct {
	ExpireSnapshots    string `cfg:"expire_snapshots"`
	RemoveOrphanFiles  string `cfg:"remove_orphan_files"`
	Optimize           string `cfg:"optimize"`
	RollbackToSnapshot string `cfg:"rollback_to_snapshot"`
}

type TaskEngineResolver struct {
	engines map[TaskKind]TaskEngine
}

func NewTaskEngineResolver(config cfg.Config) (*TaskEngineResolver, error) {
	settings := TaskEngineSettings{
		ExpireSnapshots:    string(TaskEngineSpark),
		RemoveOrphanFiles:  string(TaskEngineSpark),
		Optimize:           string(TaskEngineSpark),
		RollbackToSnapshot: string(TaskEngineSpark),
	}

	if err := config.UnmarshalKey("tasks.engines", &settings); err != nil {
		return nil, fmt.Errorf("could not unmarshal task engine settings: %w", err)
	}

	engines := map[TaskKind]TaskEngine{
		TaskKindExpireSnapshots:    TaskEngine(settings.ExpireSnapshots),
		TaskKindRemoveOrphanFiles:  TaskEngine(settings.RemoveOrphanFiles),
		TaskKindOptimize:           TaskEngine(settings.Optimize),
		TaskKindRollbackToSnapshot: TaskEngine(settings.RollbackToSnapshot),
	}

	for kind, engine := range engines {
		if err := validateTaskEngine(kind, engine); err != nil {
			return nil, err
		}
	}

	return &TaskEngineResolver{engines: engines}, nil
}

func (r *TaskEngineResolver) Resolve(kind TaskKind) (TaskEngine, error) {
	engine, ok := r.engines[kind]
	if !ok {
		return "", fmt.Errorf("unknown task kind %s", kind)
	}

	return engine, nil
}

func validateTaskEngine(kind TaskKind, engine TaskEngine) error {
	if engine != TaskEngineSpark {
		return fmt.Errorf("invalid engine %q configured for task kind %s", engine, kind)
	}

	return nil
}
