package main

import (
	"context"
	"fmt"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type ExpireSnapshotsResult struct {
	Table                string `json:"table"`
	RetentionDays        int    `json:"retention_days"`
	RetainLast           int    `json:"retain_last"`
	CleanExpiredMetadata bool   `json:"clean_expired_metadata"`
	Status               string `json:"status"`
}

type RemoveOrphanFilesResult struct {
	Table              string         `json:"table"`
	RetentionThreshold string         `json:"retention_threshold"`
	Metrics            map[string]any `json:"metrics"`
	Status             string         `json:"status"`
}

func NewServiceMaintenance(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceMaintenance, error) {
	var err error
	var trino *TrinoClient

	if trino, err = ProvideTrinoClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create trino client: %w", err)
	}

	return &ServiceMaintenance{
		logger: logger.WithChannel("maintenance"),
		trino:  trino,
	}, nil
}

type ServiceMaintenance struct {
	logger log.Logger
	trino  *TrinoClient
}

func (s *ServiceMaintenance) ExpireSnapshots(ctx context.Context, table string, retentionDays int, retainLast int) (*ExpireSnapshotsResult, error) {
	if retentionDays < 1 {
		return nil, fmt.Errorf("retention days must be at least 1")
	}

	if retainLast < 1 {
		return nil, fmt.Errorf("retain last must be at least 1")
	}

	retentionThreshold := fmt.Sprintf("%dd", retentionDays)
	qualifiedTable := qualifiedTableName("lakehouse", "main", table)
	query := fmt.Sprintf("ALTER TABLE %s EXECUTE expire_snapshots(retention_threshold => %s, retain_last => %d, clean_expired_metadata => true)", qualifiedTable, quoteLiteral(retentionThreshold), retainLast)

	if err := s.trino.Exec(ctx, query); err != nil {
		return nil, fmt.Errorf("could not expire snapshots for table %s: %w", table, err)
	}

	return &ExpireSnapshotsResult{
		Table:                table,
		RetentionDays:        retentionDays,
		RetainLast:           retainLast,
		CleanExpiredMetadata: true,
		Status:               "ok",
	}, nil
}

func (s *ServiceMaintenance) RemoveOrphanFiles(ctx context.Context, table string, retentionThreshold string) (*RemoveOrphanFilesResult, error) {
	if retentionThreshold == "" {
		return nil, fmt.Errorf("retention threshold must be non-empty")
	}

	qualifiedTable := qualifiedTableName("lakehouse", "main", table)
	query := fmt.Sprintf("ALTER TABLE %s EXECUTE remove_orphan_files(retention_threshold => %s)", qualifiedTable, quoteLiteral(retentionThreshold))

	var rows []map[string]any
	var err error

	if rows, err = s.trino.QueryRows(ctx, query); err != nil {
		return nil, fmt.Errorf("could not remove orphan files for table %s: %w", table, err)
	}

	metrics := make(map[string]any)
	for _, row := range rows {
		// Trino returns metric_name (varchar) and metric_value (bigint)
		name, okName := row["metric_name"].(string)
		val, okVal := row["metric_value"]

		if okName && okVal {
			metrics[name] = val
		}
	}

	return &RemoveOrphanFilesResult{
		Table:              table,
		RetentionThreshold: retentionThreshold,
		Metrics:            metrics,
		Status:             "ok",
	}, nil
}
