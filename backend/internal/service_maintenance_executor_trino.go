package internal

import (
	"context"
	"fmt"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type ExpireSnapshotsResult struct {
	Table                string `json:"table"`
	RetentionDays        int    `json:"retention_days"`
	CleanExpiredMetadata bool   `json:"clean_expired_metadata"`
	Status               string `json:"status"`
}

type RemoveOrphanFilesResult struct {
	Table         string         `json:"table"`
	RetentionDays int            `json:"retention_days"`
	Metrics       map[string]any `json:"metrics"`
	Status        string         `json:"status"`
}

type TrinoMaintenanceExecutor struct {
	logger    log.Logger
	trino     *TrinoClient
	metadata  *ServiceMetadata
	taskQueue TaskClaimer
	refresher SnapshotRefresher
	sqlClient sqlc.Client
}

func NewTrinoMaintenanceExecutor(ctx context.Context, config cfg.Config, logger log.Logger) (*TrinoMaintenanceExecutor, error) {
	var err error
	var trino *TrinoClient
	var metadata *ServiceMetadata
	var taskQueue TaskClaimer
	var refresher SnapshotRefresher
	var sqlClient sqlc.Client

	if trino, err = ProvideTrinoClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create trino client: %w", err)
	}

	if metadata, err = NewServiceMetadata(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create metadata service: %w", err)
	}

	if taskQueue, err = NewServiceTaskQueue(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create task queue service: %w", err)
	}

	if refresher, err = NewServiceRefresh(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create refresh service: %w", err)
	}

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}

	return &TrinoMaintenanceExecutor{
		logger:    logger.WithChannel("maintenance_executor_trino"),
		trino:     trino,
		metadata:  metadata,
		taskQueue: taskQueue,
		refresher: refresher,
		sqlClient: sqlClient,
	}, nil
}

func (s *TrinoMaintenanceExecutor) Engine() TaskEngine {
	return TaskEngineTrino
}

func (s *TrinoMaintenanceExecutor) Run(ctx context.Context) error {
	<-ctx.Done()

	return nil
}

func (s *TrinoMaintenanceExecutor) ProcessTask(ctx context.Context, task *Task) error {
	input := task.Input.Get()

	switch TaskKind(task.Kind) {
	case TaskKindExpireSnapshots:
		return s.processExpireSnapshots(ctx, task, input)
	case TaskKindRemoveOrphanFiles:
		return s.processRemoveOrphanFiles(ctx, task, input)
	case TaskKindOptimize:
		return s.taskQueue.CompleteTask(ctx, task.Id, nil, fmt.Errorf("task kind %s is not supported by engine %s", TaskKind(task.Kind), s.Engine()))
	default:
		return s.taskQueue.CompleteTask(ctx, task.Id, nil, fmt.Errorf("unknown task kind: %s", task.Kind))
	}
}

func (s *TrinoMaintenanceExecutor) processExpireSnapshots(ctx context.Context, task *Task, input map[string]any) error {
	retentionDays, _ := input["retention_days"].(float64)

	res, err := s.executeExpireSnapshots(ctx, task.Table, int(retentionDays))
	if err != nil {
		return s.taskQueue.CompleteTask(ctx, task.Id, nil, err)
	}

	err = s.sqlClient.WithTx(ctx, func(cttx sqlc.Tx) error {
		_, err := s.refresher.RefreshSnapshots(cttx, task.Table)

		return err
	})
	if err != nil {
		s.logger.Warn(ctx, "failed to refresh snapshots after expiring for table %s: %s", task.Table, err)
	}

	return s.taskQueue.CompleteTask(ctx, task.Id, expireSnapshotsResultMap(res), nil)
}

func (s *TrinoMaintenanceExecutor) processRemoveOrphanFiles(ctx context.Context, task *Task, input map[string]any) error {
	retentionDays, _ := input["retention_days"].(float64)

	res, err := s.executeRemoveOrphanFiles(ctx, task.Table, int(retentionDays))
	if err != nil {
		return s.taskQueue.CompleteTask(ctx, task.Id, nil, err)
	}

	return s.taskQueue.CompleteTask(ctx, task.Id, removeOrphanFilesResultMap(res), nil)
}

func (s *TrinoMaintenanceExecutor) executeExpireSnapshots(ctx context.Context, table string, retentionDays int) (*ExpireSnapshotsResult, error) {
	if retentionDays < 1 {
		return nil, fmt.Errorf("retention days must be at least 1")
	}

	retentionThreshold := fmt.Sprintf("%dd", retentionDays)
	qualifiedTable := qualifiedTableName("lakehouse", "main", table)
	query := fmt.Sprintf("ALTER TABLE %s EXECUTE expire_snapshots(retention_threshold => %s, clean_expired_metadata => true)", qualifiedTable, quoteLiteral(retentionThreshold))

	if err := s.trino.Exec(ctx, query); err != nil {
		return nil, fmt.Errorf("could not expire snapshots for table %s: %w", table, err)
	}

	return &ExpireSnapshotsResult{
		Table:                table,
		RetentionDays:        retentionDays,
		CleanExpiredMetadata: true,
		Status:               statusOK,
	}, nil
}

func (s *TrinoMaintenanceExecutor) executeRemoveOrphanFiles(ctx context.Context, table string, retentionDays int) (*RemoveOrphanFilesResult, error) {
	if retentionDays < 1 {
		return nil, fmt.Errorf("retention days must be at least 1")
	}

	var rows []map[string]any
	var err error

	retentionThreshold := fmt.Sprintf("%dd", retentionDays)
	qualifiedTable := qualifiedTableName("lakehouse", "main", table)
	query := fmt.Sprintf("ALTER TABLE %s EXECUTE remove_orphan_files(retention_threshold => %s)", qualifiedTable, quoteLiteral(retentionThreshold))

	if rows, err = s.trino.QueryRows(ctx, query); err != nil {
		return nil, fmt.Errorf("could not remove orphan files for table %s: %w", table, err)
	}

	metrics := make(map[string]any)
	for _, row := range rows {
		name, okName := row["metric_name"].(string)
		val, okVal := row["metric_value"]

		if okName && okVal {
			metrics[name] = val
		}
	}

	return &RemoveOrphanFilesResult{
		Table:         table,
		RetentionDays: retentionDays,
		Metrics:       metrics,
		Status:        statusOK,
	}, nil
}
