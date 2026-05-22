package internal

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type BatchExpireSnapshotsInput struct {
	Database      string   `uri:"database"`
	Tables        []string `json:"tables"`
	RetentionDays int      `json:"retention_days"`
}

type BatchRemoveOrphanFilesInput struct {
	Database      string   `uri:"database"`
	Tables        []string `json:"tables"`
	RetentionDays int      `json:"retention_days"`
}

type BatchOptimizeTableInput struct {
	Table   string `json:"table"`
	ChunkBy string `json:"chunk_by"`
}

type BatchOptimizeInput struct {
	Database         string                    `uri:"database"`
	Tables           []BatchOptimizeTableInput `json:"tables"`
	TargetFileSizeMb int                       `json:"target_file_size_mb"`
	From             DateTime                  `json:"from"`
	To               DateTime                  `json:"to"`
}

func NewHandlerMaintenance(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerMaintenance, error) {
	var err error
	var serviceTasks *ServiceTasks

	if serviceTasks, err = NewServiceTasks(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create maintenance service: %w", err)
	}

	return &HandlerMaintenance{
		serviceTasks: serviceTasks,
	}, nil
}

type HandlerMaintenance struct {
	serviceTasks *ServiceTasks
}

func (h *HandlerMaintenance) ExpireSnapshots(ctx context.Context, input *BatchExpireSnapshotsInput) (httpserver.Response, error) {
	var err error
	var result *BatchEnqueueResult

	if result, err = h.serviceTasks.EnqueueExpireSnapshotsBatch(ctx, input.Database, input.Tables, input.RetentionDays); err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(result), nil
}

func (h *HandlerMaintenance) RemoveOrphanFiles(ctx context.Context, input *BatchRemoveOrphanFilesInput) (httpserver.Response, error) {
	var err error
	var result *BatchEnqueueResult

	if result, err = h.serviceTasks.EnqueueRemoveOrphanFilesBatch(ctx, input.Database, input.Tables, input.RetentionDays); err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(result), nil
}

func (h *HandlerMaintenance) Optimize(ctx context.Context, input *BatchOptimizeInput) (httpserver.Response, error) {
	var err error
	var result *BatchEnqueueResult

	tables := make([]BatchOptimizeTable, 0, len(input.Tables))
	for _, table := range input.Tables {
		tables = append(tables, BatchOptimizeTable(table))
	}

	if result, err = h.serviceTasks.EnqueueOptimizeBatch(ctx, input.Database, tables, input.TargetFileSizeMb, input.From.Time, input.To.Time); err != nil {
		return nil, err
	}

	return httpserver.NewJsonResponse(result), nil
}
