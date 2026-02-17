package internal

import (
	"context"
	"time"

	"github.com/gosoline-project/sqlc"
)

// TaskClaimer abstracts task queue operations used by the task worker.
type TaskClaimer interface {
	ClaimTask(ctx context.Context) (*Task, error)
	CompleteTask(ctx context.Context, id int64, result map[string]any, err error) error
}

// MaintenanceExecutor abstracts maintenance execution operations.
type MaintenanceExecutor interface {
	ExecuteExpireSnapshots(ctx context.Context, table string, retentionDays int, retainLast int) (*ExpireSnapshotsResult, error)
	ExecuteRemoveOrphanFiles(ctx context.Context, table string, retentionDays int) (*RemoveOrphanFilesResult, error)
	ExecuteOptimize(ctx context.Context, table string, fileSizeThresholdMb int, from time.Time, to time.Time) (*OptimizeResult, error)
}

// SnapshotRefresher abstracts the snapshot refresh operation.
type SnapshotRefresher interface {
	RefreshSnapshots(cttx sqlc.Tx, table string) ([]Snapshot, error)
}
