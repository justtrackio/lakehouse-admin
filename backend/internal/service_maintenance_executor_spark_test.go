package internal

import (
	"context"
	"testing"

	"github.com/justtrackio/gosoline/pkg/db"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/stretchr/testify/require"
)

type recordingTaskQueue struct {
	TaskClaimer
	requeued  []int64
	completed map[int64]error
}

func (q *recordingTaskQueue) RequeueTask(_ context.Context, id int64) error {
	q.requeued = append(q.requeued, id)

	return nil
}

func (q *recordingTaskQueue) CompleteTask(_ context.Context, id int64, _ map[string]any, err error) error {
	q.completed[id] = err

	return nil
}

func TestReconcileUnsubmittedTaskFailsRollbacksAndRequeuesTheRest(t *testing.T) {
	queue := &recordingTaskQueue{completed: map[int64]error{}}
	executor := &SparkMaintenanceExecutor{logger: log.NewLogger(), taskQueue: queue}
	task := func(id int64, kind TaskKind, input map[string]any) *Task {
		return &Task{Id: id, Kind: string(kind), Input: db.NewJSON(input, db.NonNullable{})}
	}

	require.NoError(t, executor.reconcileUnsubmittedTask(context.Background(), task(1, TaskKindRollbackToSnapshot, map[string]any{"snapshot_id": "7"})))
	require.NoError(t, executor.reconcileUnsubmittedTask(context.Background(), task(2, TaskKindRollbackToSnapshot, map[string]any{"snapshot_id": "7", taskInputRefreshOnly: true})))
	require.NoError(t, executor.reconcileUnsubmittedTask(context.Background(), task(3, TaskKindExpireSnapshots, map[string]any{"retention_days": 7})))

	require.Equal(t, []int64{2, 3}, queue.requeued)
	require.Len(t, queue.completed, 1)
	require.ErrorContains(t, queue.completed[1], "unknown whether its rollback ran")
}
