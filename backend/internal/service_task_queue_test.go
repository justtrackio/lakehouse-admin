package internal

import (
	"testing"

	"github.com/justtrackio/gosoline/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestRetryInputRepeatsOnlyTheRefreshAfterSparkSucceeded(t *testing.T) {
	task := &Task{
		Kind:   string(TaskKindRollbackToSnapshot),
		Input:  db.NewJSON(map[string]any{"snapshot_id": "6548219920550331090"}, db.NonNullable{}),
		Result: db.NewJSON(map[string]any{taskResultSparkSucceeded: true}, db.NonNullable{}),
	}

	require.Equal(t, map[string]any{"snapshot_id": "6548219920550331090", taskInputRefreshOnly: true}, retryInput(task))
	require.Equal(t, map[string]any{"snapshot_id": "6548219920550331090"}, task.Input.Get())

	task.Result = db.NewJSON(map[string]any{"status": statusError}, db.NonNullable{})
	require.Equal(t, map[string]any{"snapshot_id": "6548219920550331090"}, retryInput(task))
}
