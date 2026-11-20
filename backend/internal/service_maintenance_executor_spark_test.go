package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/justtrackio/gosoline/pkg/db"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

type flakyApplications struct {
	SparkApplicationCreator
	createErr error
	existing  *SparkApplicationManifest
	getErr    error
}

func (a flakyApplications) CreateSparkApplication(context.Context, *SparkApplicationManifest) (*SparkApplicationManifest, error) {
	return nil, a.createErr
}

func (a flakyApplications) GetSparkApplication(context.Context, string, string) (*SparkApplicationManifest, error) {
	return a.existing, a.getErr
}

func TestSubmitSparkApplicationResolvesALostCreateResponse(t *testing.T) {
	manifest := &SparkApplicationManifest{}
	manifest.Metadata.Name = "expire-snapshots-events-7"
	created := &SparkApplicationManifest{}
	created.Metadata.Annotations = map[string]string{sparkApplicationTaskIDAnnotation: "7"}
	submit := func(k8s SparkApplicationCreator) error {
		executor := &SparkMaintenanceExecutor{logger: log.NewLogger(), k8s: k8s}

		return executor.submitSparkApplication(context.Background(), manifest, 7)
	}

	timeout := errors.New("context deadline exceeded")
	invalid := k8serrors.NewInvalid(schema.GroupKind{Group: "spark.apache.org", Kind: "SparkApplication"}, manifest.Metadata.Name, nil)
	exists := k8serrors.NewAlreadyExists(schema.GroupResource{Group: "spark.apache.org", Resource: "sparkapplications"}, manifest.Metadata.Name)
	other := &SparkApplicationManifest{}
	other.Metadata.Annotations = map[string]string{sparkApplicationTaskIDAnnotation: "8"}

	// An ambiguous create stays unknown, even if the application isn't there yet.
	require.NoError(t, submit(flakyApplications{createErr: timeout, existing: created}))
	require.ErrorIs(t, submit(flakyApplications{createErr: timeout}), errSparkSubmissionUnknown)
	require.ErrorIs(t, submit(flakyApplications{createErr: timeout, getErr: errors.New("reset")}), errSparkSubmissionUnknown)

	// Definitive answers settle it.
	require.ErrorIs(t, submit(flakyApplications{createErr: invalid}), invalid)
	require.NoError(t, submit(flakyApplications{createErr: exists, existing: created}))
	require.ErrorIs(t, submit(flakyApplications{createErr: exists, existing: other}), exists)
}
