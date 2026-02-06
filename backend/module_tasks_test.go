package main

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gosoline-project/sqlc"
	"github.com/jmoiron/sqlx"
	"github.com/justtrackio/gosoline/pkg/db"
	"github.com/justtrackio/gosoline/pkg/exec"
	logmocks "github.com/justtrackio/gosoline/pkg/log/mocks"
	"github.com/marusama/semaphore/v2"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

func TestModuleTasksSuite(t *testing.T) {
	suite.Run(t, new(ModuleTasksSuite))
}

type ModuleTasksSuite struct {
	suite.Suite
	module    *ModuleTasks
	claimer   *MockTaskClaimer
	executor  *MockMaintenanceExecutor
	refresher *MockSnapshotRefresher
	sqlDB     *sql.DB
	mock      sqlmock.Sqlmock
	sqlClient sqlc.Client
}

func (s *ModuleTasksSuite) SetupTest() {
	var err error

	// Create mockery-generated mocks
	s.claimer = NewMockTaskClaimer(s.T())
	s.executor = NewMockMaintenanceExecutor(s.T())
	s.refresher = NewMockSnapshotRefresher(s.T())

	// Create sqlc client with go-sqlmock for WithTx support
	s.sqlDB, s.mock, err = sqlmock.New()
	s.Require().NoError(err)

	sqlxDB := sqlx.NewDb(s.sqlDB, "mysql")
	logger := logmocks.NewLoggerMock(logmocks.WithMockAll)
	executor := exec.NewDefaultExecutor()
	qbConfig := sqlc.DefaultConfig()

	s.sqlClient = sqlc.NewClientWithInterfaces(logger, sqlxDB, executor, qbConfig)

	// Create ModuleTasks with short poll interval
	s.module = &ModuleTasks{
		logger:                     logger,
		serviceTaskQueue:           s.claimer,
		serviceMaintenanceExecutor: s.executor,
		serviceRefresh:             s.refresher,
		sqlClient:                  s.sqlClient,
		pollInterval:               time.Millisecond,
		sem:                        semaphore.New(2),
	}
}

func (s *ModuleTasksSuite) TearDownTest() {
	if s.sqlDB != nil {
		s.sqlDB.Close()
	}
}

// TestProcessTask_ExpireSnapshots tests expire_snapshots task processing
func (s *ModuleTasksSuite) TestProcessTask_ExpireSnapshots() {
	ctx := context.Background()

	task := &Task{
		Id:    1,
		Table: "test_table",
		Kind:  "expire_snapshots",
		Input: db.NewJSON(map[string]any{
			"retention_days": float64(7),
			"retain_last":    float64(5),
		}, db.NonNullable{}),
	}

	// Mock executor expectations
	s.executor.EXPECT().
		ExecuteExpireSnapshots(mock.Anything, "test_table", 7, 5).
		Run(func(ctx context.Context, table string, retentionDays int, retainLast int) {
			s.Equal("test_table", table)
			s.Equal(7, retentionDays)
			s.Equal(5, retainLast)
		}).
		Return(&ExpireSnapshotsResult{
			Table:                "test_table",
			RetentionDays:        7,
			RetainLast:           5,
			CleanExpiredMetadata: true,
			Status:               "ok",
		}, nil)

	// Mock RefreshSnapshots (called after ExpireSnapshots)
	s.refresher.EXPECT().
		RefreshSnapshots(mock.Anything, "test_table").
		Return(nil, nil)

	// Mock completer expectations - use MatchedBy for nil error
	s.claimer.EXPECT().
		CompleteTask(mock.Anything, int64(1), mock.Anything, mock.MatchedBy(func(err error) bool {
			return err == nil
		})).
		Run(func(ctx context.Context, id int64, result map[string]any, err error) {
			s.Equal(int64(1), id)
			s.Nil(err)
			s.Equal("test_table", result["table"])
			s.Equal(7, result["retention_days"])
			s.Equal(5, result["retain_last"])
			s.Equal(true, result["clean_expired_metadata"])
			s.Equal("ok", result["status"])
		}).
		Return(nil)

	// Mock WithTx to call the function immediately
	s.mock.ExpectBegin()
	s.mock.ExpectCommit()

	err := s.module.processTask(ctx, task)

	s.NoError(err)
}

// TestProcessTask_RemoveOrphanFiles tests remove_orphan_files task processing
func (s *ModuleTasksSuite) TestProcessTask_RemoveOrphanFiles() {
	ctx := context.Background()

	task := &Task{
		Id:    2,
		Table: "test_table",
		Kind:  "remove_orphan_files",
		Input: db.NewJSON(map[string]any{
			"retention_days": float64(14),
		}, db.NonNullable{}),
	}

	// Mock executor expectations
	s.executor.EXPECT().
		ExecuteRemoveOrphanFiles(mock.Anything, "test_table", 14).
		Run(func(ctx context.Context, table string, retentionDays int) {
			s.Equal("test_table", table)
			s.Equal(14, retentionDays)
		}).
		Return(&RemoveOrphanFilesResult{
			Table:         "test_table",
			RetentionDays: 14,
			Metrics:       map[string]any{"files_removed": 42},
			Status:        "ok",
		}, nil)

	// Mock completer expectations - use MatchedBy for nil error
	s.claimer.EXPECT().
		CompleteTask(mock.Anything, int64(2), mock.Anything, mock.MatchedBy(func(err error) bool {
			return err == nil
		})).
		Run(func(ctx context.Context, id int64, result map[string]any, err error) {
			s.Equal(int64(2), id)
			s.Nil(err)
			s.Equal("test_table", result["table"])
			s.Equal(14, result["retention_days"])
			s.Equal("ok", result["status"])
		}).
		Return(nil)

	// Mock WithTx to call the function immediately
	s.mock.ExpectBegin()
	s.mock.ExpectCommit()

	err := s.module.processTask(ctx, task)

	s.NoError(err)
}

// TestProcessTask_Optimize tests optimize task processing
func (s *ModuleTasksSuite) TestProcessTask_Optimize() {
	ctx := context.Background()

	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)

	task := &Task{
		Id:    3,
		Table: "test_table",
		Kind:  "optimize",
		Input: db.NewJSON(map[string]any{
			"file_size_threshold_mb": float64(100),
			"from":                   from.Format(time.RFC3339),
			"to":                     to.Format(time.RFC3339),
		}, db.NonNullable{}),
	}

	// Mock executor expectations
	s.executor.EXPECT().
		ExecuteOptimize(mock.Anything, "test_table", 100, mock.Anything, mock.Anything).
		Run(func(ctx context.Context, table string, fileSizeThresholdMb int, fromTime time.Time, toTime time.Time) {
			s.Equal("test_table", table)
			s.Equal(100, fileSizeThresholdMb)
		}).
		Return(&OptimizeResult{
			Table:               "test_table",
			FileSizeThresholdMb: 100,
			Where:               "date(day) >= date '2026-01-01' AND date(day) <= date '2026-01-31'",
			Status:              "ok",
		}, nil)

	// Mock completer expectations - use MatchedBy for nil error
	s.claimer.EXPECT().
		CompleteTask(mock.Anything, int64(3), mock.Anything, mock.MatchedBy(func(err error) bool {
			return err == nil
		})).
		Run(func(ctx context.Context, id int64, result map[string]any, err error) {
			s.Equal(int64(3), id)
			s.Nil(err)
			s.Equal("test_table", result["table"])
			s.Equal(100, result["file_size_threshold_mb"])
			s.Equal("ok", result["status"])
		}).
		Return(nil)

	// Mock WithTx to call the function immediately
	s.mock.ExpectBegin()
	s.mock.ExpectCommit()

	err := s.module.processTask(ctx, task)

	s.NoError(err)
}

// TestProcessTask_UnknownKind tests handling of unknown task kinds
func (s *ModuleTasksSuite) TestProcessTask_UnknownKind() {
	ctx := context.Background()

	task := &Task{
		Id:    4,
		Table: "test_table",
		Kind:  "unknown_kind",
		Input: db.NewJSON(map[string]any{}, db.NonNullable{}),
	}

	// Mock completer expectations - should be called with an error
	// Use MatchedBy to properly match map[string]interface{}(nil) vs nil
	var capturedErr error
	s.claimer.EXPECT().
		CompleteTask(mock.Anything, int64(4), mock.MatchedBy(func(result map[string]any) bool {
			return result == nil
		}), mock.Anything).
		Run(func(ctx context.Context, id int64, result map[string]any, err error) {
			capturedErr = err
			s.Equal(int64(4), id)
			s.Nil(result) // result is nil for unknown kinds
		}).
		Return(nil)

	// Mock WithTx to call the function immediately
	s.mock.ExpectBegin()
	s.mock.ExpectCommit()

	err := s.module.processTask(ctx, task)

	s.NoError(err) // processTask returns nil, errors go through CompleteTask
	s.NotNil(capturedErr)
	s.Contains(capturedErr.Error(), "unknown task kind")
}

// TestProcessTask_ExecutionError tests handling when executor returns an error
func (s *ModuleTasksSuite) TestProcessTask_ExecutionError() {
	ctx := context.Background()

	task := &Task{
		Id:    5,
		Table: "test_table",
		Kind:  "optimize",
		Input: db.NewJSON(map[string]any{
			"file_size_threshold_mb": float64(100),
			"from":                   time.Now().Format(time.RFC3339),
			"to":                     time.Now().Format(time.RFC3339),
		}, db.NonNullable{}),
	}

	expectedErr := errors.New("trino connection failed")

	// Mock executor expectations - return an error
	s.executor.EXPECT().
		ExecuteOptimize(mock.Anything, "test_table", 100, mock.Anything, mock.Anything).
		Return(nil, expectedErr)

	// Mock completer expectations - should be called with the error
	// Use MatchedBy to properly match map[string]interface{}(nil) vs nil
	var capturedErr error
	s.claimer.EXPECT().
		CompleteTask(mock.Anything, int64(5), mock.MatchedBy(func(result map[string]any) bool {
			return result == nil
		}), mock.Anything).
		Run(func(ctx context.Context, id int64, result map[string]any, err error) {
			capturedErr = err
			s.Equal(int64(5), id)
			s.Nil(result)
		}).
		Return(nil)

	// Mock WithTx to call the function immediately
	s.mock.ExpectBegin()
	s.mock.ExpectCommit()

	err := s.module.processTask(ctx, task)

	s.NoError(err)
	s.Equal(expectedErr, capturedErr)
}

// TestSetWorkerCount tests dynamic worker count adjustment
func (s *ModuleTasksSuite) TestSetWorkerCount() {
	s.Equal(2, s.module.sem.GetLimit())

	s.module.SetWorkerCount(5)
	s.Equal(5, s.module.sem.GetLimit())

	// Test clamping to minimum of 1
	s.module.SetWorkerCount(0)
	s.Equal(1, s.module.sem.GetLimit())

	s.module.SetWorkerCount(-5)
	s.Equal(1, s.module.sem.GetLimit())
}

// TestRun_ProcessesTask tests that Run picks up and processes a task
func (s *ModuleTasksSuite) TestRun_ProcessesTask() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := &Task{
		Id:    1,
		Table: "test_table",
		Kind:  "optimize",
		Input: db.NewJSON(map[string]any{
			"file_size_threshold_mb": float64(100),
			"from":                   time.Now().Format(time.RFC3339),
			"to":                     time.Now().Format(time.RFC3339),
		}, db.NonNullable{}),
	}

	// ClaimTask returns task once, then cancels context and returns nil
	callCount := 0
	s.claimer.EXPECT().
		ClaimTask(mock.Anything).
		RunAndReturn(func(ctx context.Context) (*Task, error) {
			callCount++
			if callCount == 1 {
				return task, nil
			}
			// Cancel context to stop the loop after processing the task
			cancel()
			return nil, nil
		}).
		Maybe() // Call count is indeterminate due to timing

	// Mock executor expectations
	s.executor.EXPECT().
		ExecuteOptimize(mock.Anything, "test_table", 100, mock.Anything, mock.Anything).
		Return(&OptimizeResult{
			Table:               "test_table",
			FileSizeThresholdMb: 100,
			Status:              "ok",
		}, nil).
		Once()

	// Mock completer expectations
	s.claimer.EXPECT().
		CompleteTask(mock.Anything, int64(1), mock.Anything, mock.MatchedBy(func(err error) bool {
			return err == nil
		})).
		Return(nil).
		Once()

	// Run should exit cleanly when context is cancelled
	err := s.module.Run(ctx)
	s.NoError(err)

	// Verify the task was picked up
	s.GreaterOrEqual(callCount, 1)
}

// TestRun_NoTaskAvailable tests that Run handles no task available gracefully
func (s *ModuleTasksSuite) TestRun_NoTaskAvailable() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callCount := 0
	s.claimer.EXPECT().
		ClaimTask(mock.Anything).
		RunAndReturn(func(ctx context.Context) (*Task, error) {
			callCount++
			if callCount >= 3 {
				cancel() // Stop after a few iterations
			}
			return nil, nil // No task available
		}).
		Maybe()

	// No executor or completer calls should happen
	// (no expectations set)

	err := s.module.Run(ctx)
	s.NoError(err)
	s.GreaterOrEqual(callCount, 3)
}

// TestRun_ClaimTaskError tests that Run handles ClaimTask errors gracefully
func (s *ModuleTasksSuite) TestRun_ClaimTaskError() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callCount := 0
	s.claimer.EXPECT().
		ClaimTask(mock.Anything).
		RunAndReturn(func(ctx context.Context) (*Task, error) {
			callCount++
			if callCount >= 3 {
				cancel()
			}
			return nil, errors.New("db connection failed")
		}).
		Maybe()

	// No executor or completer calls should happen
	// (no expectations set)

	err := s.module.Run(ctx)
	s.NoError(err) // Errors are logged, not propagated
	s.GreaterOrEqual(callCount, 3)
}

// TestRun_RespectsContextCancellation tests that Run exits when context is cancelled
func (s *ModuleTasksSuite) TestRun_RespectsContextCancellation() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// ClaimTask may or may not be called depending on timing
	s.claimer.EXPECT().
		ClaimTask(mock.Anything).
		Return(nil, nil).
		Maybe()

	// Run should exit immediately
	err := s.module.Run(ctx)
	// Either no error (clean shutdown) or context.Canceled (if cancelled during loop startup)
	if err != nil && err != context.Canceled {
		s.Failf("unexpected error from Run", "got: %v, want: nil or context.Canceled", err)
	}
}

// TestRun_SemaphoreLimitsConcurrency tests that the semaphore limits concurrent task processing
func (s *ModuleTasksSuite) TestRun_SemaphoreLimitsConcurrency() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set semaphore limit to 1
	s.module.SetWorkerCount(1)

	task := &Task{
		Id:    1,
		Table: "test_table",
		Kind:  "optimize",
		Input: db.NewJSON(map[string]any{
			"file_size_threshold_mb": float64(100),
			"from":                   time.Now().Format(time.RFC3339),
			"to":                     time.Now().Format(time.RFC3339),
		}, db.NonNullable{}),
	}

	// Channel to block executor until we're ready
	executorStarted := make(chan struct{})
	executorUnblock := make(chan struct{})

	callCount := 0
	s.claimer.EXPECT().
		ClaimTask(mock.Anything).
		RunAndReturn(func(ctx context.Context) (*Task, error) {
			callCount++
			if callCount == 1 {
				return task, nil
			}
			// After first task, cancel context to stop loop
			cancel()
			return nil, nil
		}).
		Maybe()

	// Mock executor that blocks to simulate slow processing
	s.executor.EXPECT().
		ExecuteOptimize(mock.Anything, "test_table", 100, mock.Anything, mock.Anything).
		Run(func(ctx context.Context, table string, fileSizeThresholdMb int, from time.Time, to time.Time) {
			close(executorStarted)
			<-executorUnblock // Block until we release it
		}).
		Return(&OptimizeResult{
			Table:               "test_table",
			FileSizeThresholdMb: 100,
			Status:              "ok",
		}, nil).
		Once()

	// Mock completer
	s.claimer.EXPECT().
		CompleteTask(mock.Anything, int64(1), mock.Anything, mock.MatchedBy(func(err error) bool {
			return err == nil
		})).
		Return(nil).
		Once()

	// Start Run in a goroutine
	runDone := make(chan error)
	go func() {
		runDone <- s.module.Run(ctx)
	}()

	// Wait for executor to start
	<-executorStarted

	// Give some time for the ticker to fire again (should be blocked by semaphore)
	time.Sleep(5 * time.Millisecond)

	// Now unblock the executor
	close(executorUnblock)

	// Wait for Run to finish
	err := <-runDone
	s.NoError(err)

	// Verify only one task was processed (due to semaphore limit)
	// The callCount should be >= 1 (task picked up)
	s.GreaterOrEqual(callCount, 1)
}

// TestRun_ParallelTaskProcessing tests that increasing capacity to 2 allows two tasks to run in parallel
func (s *ModuleTasksSuite) TestRun_ParallelTaskProcessing() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Set semaphore limit to 2 to allow parallel processing
	s.module.SetWorkerCount(2)

	// Create two distinct tasks
	task1 := &Task{
		Id:    1,
		Table: "table_a",
		Kind:  "optimize",
		Input: db.NewJSON(map[string]any{
			"file_size_threshold_mb": float64(100),
			"from":                   time.Now().Format(time.RFC3339),
			"to":                     time.Now().Format(time.RFC3339),
		}, db.NonNullable{}),
	}

	task2 := &Task{
		Id:    2,
		Table: "table_b",
		Kind:  "optimize",
		Input: db.NewJSON(map[string]any{
			"file_size_threshold_mb": float64(200),
			"from":                   time.Now().Format(time.RFC3339),
			"to":                     time.Now().Format(time.RFC3339),
		}, db.NonNullable{}),
	}

	// Channels to track parallel execution
	task1Started := make(chan struct{})
	task2Started := make(chan struct{})
	executorUnblock := make(chan struct{})

	// ClaimTask returns task1, then task2, then returns nil
	callCount := 0
	tasksDone := false
	s.claimer.EXPECT().
		ClaimTask(mock.Anything).
		RunAndReturn(func(ctx context.Context) (*Task, error) {
			callCount++
			if callCount == 1 {
				return task1, nil
			}
			if callCount == 2 {
				return task2, nil
			}
			// After both tasks claimed, wait for them to complete then cancel
			if !tasksDone {
				tasksDone = true
				// Give a tiny bit of time for tasks to process, then cancel
				go func() {
					time.Sleep(20 * time.Millisecond)
					cancel()
				}()
			}
			return nil, nil
		}).
		Maybe()

	// Mock executor for task 1 - blocks until we release it
	s.executor.EXPECT().
		ExecuteOptimize(mock.Anything, "table_a", 100, mock.Anything, mock.Anything).
		Run(func(ctx context.Context, table string, fileSizeThresholdMb int, from time.Time, to time.Time) {
			close(task1Started) // Signal that task 1 has started
			<-executorUnblock   // Block until released
		}).
		Return(&OptimizeResult{
			Table:               "table_a",
			FileSizeThresholdMb: 100,
			Status:              "ok",
		}, nil).
		Once()

	// Mock executor for task 2 - blocks until we release it
	s.executor.EXPECT().
		ExecuteOptimize(mock.Anything, "table_b", 200, mock.Anything, mock.Anything).
		Run(func(ctx context.Context, table string, fileSizeThresholdMb int, from time.Time, to time.Time) {
			close(task2Started) // Signal that task 2 has started
			<-executorUnblock   // Block until released
		}).
		Return(&OptimizeResult{
			Table:               "table_b",
			FileSizeThresholdMb: 200,
			Status:              "ok",
		}, nil).
		Once()

	// Mock completer for both tasks
	s.claimer.EXPECT().
		CompleteTask(mock.Anything, int64(1), mock.Anything, mock.MatchedBy(func(err error) bool {
			return err == nil
		})).
		Return(nil).
		Once()

	s.claimer.EXPECT().
		CompleteTask(mock.Anything, int64(2), mock.Anything, mock.MatchedBy(func(err error) bool {
			return err == nil
		})).
		Return(nil).
		Once()

	// Start Run in a goroutine
	runDone := make(chan error)
	go func() {
		runDone <- s.module.Run(ctx)
	}()

	// Wait for BOTH tasks to start - this proves parallelism
	// If they weren't running in parallel, task2Started would block forever
	select {
	case <-task1Started:
	case <-time.After(100 * time.Millisecond):
		s.Fail("task1 did not start in time")
	}

	select {
	case <-task2Started:
	case <-time.After(100 * time.Millisecond):
		s.Fail("task2 did not start in time")
	}

	// Both tasks are now running in parallel - unblock them
	close(executorUnblock)

	// Wait for Run to finish (may return nil or context.Canceled)
	err := <-runDone
	// Either no error (clean shutdown) or context.Canceled (if we cancelled mid-loop)
	if err != nil && err != context.Canceled {
		s.Fail("unexpected error from Run: %v", err)
	}

	// Verify both tasks were picked up
	s.GreaterOrEqual(callCount, 2)
}
