package internal

import (
	"context"
	"testing"
	"time"

	"github.com/justtrackio/gosoline/pkg/db"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/stretchr/testify/require"
)

type recordingTaskQueue struct {
	TaskClaimer
	running   []Task
	requeued  []int64
	completed map[int64]error
}

func (q *recordingTaskQueue) ListRunningTasks(context.Context, TaskEngine) ([]Task, error) {
	return q.running, nil
}

func (q *recordingTaskQueue) RequeueTask(_ context.Context, id int64) error {
	q.requeued = append(q.requeued, id)

	return nil
}

func (q *recordingTaskQueue) CompleteTask(_ context.Context, id int64, _ map[string]any, err error) error {
	q.completed[id] = err

	return nil
}

type submittedApplications struct {
	SparkApplicationCreator
	taskIDs map[int64]bool
}

func (a submittedApplications) ListSparkApplicationTaskIDs(context.Context) (map[int64]bool, error) {
	return a.taskIDs, nil
}

func TestReconcileLeavesActiveClaimsAndSubmittedTasksAlone(t *testing.T) {
	recent := time.Now().Add(-time.Minute)
	abandoned := time.Now().Add(-reconcileClaimGrace - time.Minute)
	task := func(id int64, pickedUp time.Time) Task {
		return Task{Id: id, Kind: string(TaskKindExpireSnapshots), PickedUpAt: &pickedUp, Input: db.NewJSON(map[string]any{}, db.NonNullable{})}
	}

	queue := &recordingTaskQueue{completed: map[int64]error{}, running: []Task{task(1, abandoned), task(2, recent), task(3, abandoned)}}
	executor := &SparkMaintenanceExecutor{logger: log.NewLogger(), taskQueue: queue, k8s: submittedApplications{taskIDs: map[int64]bool{3: true}}}

	require.NoError(t, executor.Reconcile(context.Background()))
	require.Equal(t, []int64{1}, queue.requeued)
}
