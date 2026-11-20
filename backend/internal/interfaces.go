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

const TaskEngineSpark TaskEngine = "spark"

// TaskClaimer abstracts task queue operations used by the task worker.
type TaskClaimer interface {
	ClaimTask(ctx context.Context) (*Task, error)
	UpdateTaskResult(ctx context.Context, id int64, result map[string]any) error
	UpdateTaskResultNested(ctx context.Context, id int64, key string, result map[string]any) error
	CompleteTask(ctx context.Context, id int64, result map[string]any, err error) error
	FailOrphanedRunningTasks(ctx context.Context, engines []TaskEngine) (int64, error)
	ListRunningTasks(ctx context.Context, engine TaskEngine) ([]Task, error)
	RequeueTask(ctx context.Context, id int64) error
	TaskStatus(ctx context.Context, id int64) (string, error)
}

type MaintenanceExecutor interface {
	Engine() TaskEngine
	Run(ctx context.Context) error
	ProcessTask(ctx context.Context, task *Task) error
	// Reconcile settles the running tasks a previous instance left behind. It
	// runs at startup, before any task is claimed.
	Reconcile(ctx context.Context) error
}

type SparkApplicationCreator interface {
	CreateSparkApplication(ctx context.Context, manifest *SparkApplicationManifest) (*SparkApplicationManifest, error)
	DeleteSparkApplication(ctx context.Context, namespace string, name string) error
	WatchSparkApplications(ctx context.Context) (cache.SharedIndexInformer, error)
	ListSparkApplicationTaskIDs(ctx context.Context) (map[int64]bool, error)
}

// SnapshotRefresher abstracts the snapshot refresh operation.
type SnapshotRefresher interface {
	RefreshSnapshots(cttx sqlc.Tx, database string, table string) ([]Snapshot, error)
}
