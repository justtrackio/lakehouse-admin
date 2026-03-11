package internal

import (
	"context"

	"github.com/gosoline-project/sqlc"
	"k8s.io/client-go/tools/cache"
)

type TaskKind string

const (
	TaskKindExpireSnapshots   TaskKind = "expire_snapshots"
	TaskKindRemoveOrphanFiles TaskKind = "remove_orphan_files"
	TaskKindOptimize          TaskKind = "optimize"
)

type TaskEngine string

const (
	TaskEngineTrino TaskEngine = "trino"
	TaskEngineSpark TaskEngine = "spark"
)

// TaskClaimer abstracts task queue operations used by the task worker.
type TaskClaimer interface {
	ClaimTask(ctx context.Context) (*Task, error)
	UpdateTaskResult(ctx context.Context, id int64, result map[string]any) error
	CompleteTask(ctx context.Context, id int64, result map[string]any, err error) error
}

type MaintenanceExecutor interface {
	Engine() TaskEngine
	Run(ctx context.Context) error
	ProcessTask(ctx context.Context, task *Task) error
}

type SparkApplicationCreator interface {
	CreateSparkApplication(ctx context.Context, manifest *SparkApplicationManifest) (*SparkApplicationManifest, error)
	WatchSparkApplications(ctx context.Context) (cache.SharedIndexInformer, error)
}

// SnapshotRefresher abstracts the snapshot refresh operation.
type SnapshotRefresher interface {
	RefreshSnapshots(cttx sqlc.Tx, table string) ([]Snapshot, error)
}
