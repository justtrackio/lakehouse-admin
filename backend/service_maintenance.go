package main

import (
	"context"
	"fmt"
	"time"

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
	Table         string         `json:"table"`
	RetentionDays int            `json:"retention_days"`
	Metrics       map[string]any `json:"metrics"`
	Status        string         `json:"status"`
}

type OptimizeResult struct {
	Table               string `json:"table"`
	FileSizeThresholdMb int    `json:"file_size_threshold_mb"`
	Where               string `json:"where"`
	Status              string `json:"status"`
}

func NewServiceMaintenance(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceMaintenance, error) {
	var err error
	var trino *TrinoClient
	var metadata *ServiceMetadata

	if trino, err = ProvideTrinoClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create trino client: %w", err)
	}

	if metadata, err = NewServiceMetadata(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create metadata service: %w", err)
	}

	return &ServiceMaintenance{
		logger:   logger.WithChannel("maintenance"),
		trino:    trino,
		metadata: metadata,
	}, nil
}

type ServiceMaintenance struct {
	logger   log.Logger
	trino    *TrinoClient
	metadata *ServiceMetadata
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

func (s *ServiceMaintenance) RemoveOrphanFiles(ctx context.Context, table string, retentionDays int) (*RemoveOrphanFilesResult, error) {
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
		Status:        "ok",
	}, nil
}

func (s *ServiceMaintenance) Optimize(ctx context.Context, table string, fileSizeThresholdMb int, from string, to string, batchSize string) (*OptimizeResult, error) {
	if fileSizeThresholdMb < 1 {
		return nil, fmt.Errorf("file size threshold must be at least 1")
	}

	var err error
	var desc *TableDescription
	var partitionColumn string
	var startDate, endDate time.Time

	if startDate, err = time.Parse(time.DateOnly, from); err != nil {
		return nil, fmt.Errorf("could not parse from date: %w", err)
	}

	if endDate, err = time.Parse(time.DateOnly, to); err != nil {
		return nil, fmt.Errorf("could not parse to date: %w", err)
	}

	if startDate.After(endDate) {
		return nil, fmt.Errorf("from date must be before or equal to to date")
	}

	if desc, err = s.metadata.GetTable(ctx, table); err != nil {
		return nil, fmt.Errorf("could not get table metadata: %w", err)
	}

	for _, p := range desc.Partitions.Get() {
		if p.IsHidden && p.Hidden.Type == "day" {
			partitionColumn = p.Hidden.Column
		}
	}

	if partitionColumn == "" {
		return nil, fmt.Errorf("no suitable day-partition column found for optimization")
	}

	threshold := fmt.Sprintf("%dMB", fileSizeThresholdMb)
	qualifiedTable := qualifiedTableName("lakehouse", "main", table)

	// We split the optimization into chunks to avoid hitting Trino limits
	current := startDate
	finalEnd := endDate

	for !current.After(finalEnd) {
		var nextStart time.Time

		switch batchSize {
		case "daily":
			nextStart = current.AddDate(0, 0, 1)
		case "weekly":
			nextStart = current.AddDate(0, 0, 7)
		case "yearly":
			nextStart = current.AddDate(1, 0, 0)
		case "monthly":
			fallthrough
		default:
			nextStart = current.AddDate(0, 1, 0)
		}

		// batchEnd is one day before nextStart
		batchEnd := nextStart.AddDate(0, 0, -1)

		// Clamp batchEnd to finalEnd
		if batchEnd.After(finalEnd) {
			batchEnd = finalEnd
		}

		batchWhere := fmt.Sprintf("date(%s) >= date '%s' AND date(%s) <= date '%s'", partitionColumn, current.Format(time.DateOnly), partitionColumn, batchEnd.Format(time.DateOnly))
		query := fmt.Sprintf("ALTER TABLE %s EXECUTE optimize(file_size_threshold => %s) WHERE %s", qualifiedTable, quoteLiteral(threshold), batchWhere)

		s.logger.Info(ctx, "optimizing table %s batch %s to %s", table, current.Format(time.DateOnly), batchEnd.Format(time.DateOnly))

		if err := s.trino.Exec(ctx, query); err != nil {
			return nil, fmt.Errorf("could not optimize table %s (batch %s): %w", table, batchWhere, err)
		}

		// Move to the next batch start
		current = nextStart
	}

	// The returned "Where" field still represents the user's original request range
	fullWhere := fmt.Sprintf("date(%s) >= date '%s' AND date(%s) <= date '%s'", partitionColumn, startDate.Format(time.DateOnly), partitionColumn, endDate.Format(time.DateOnly))

	return &OptimizeResult{
		Table:               table,
		FileSizeThresholdMb: fileSizeThresholdMb,
		Where:               fullWhere,
		Status:              "ok",
	}, nil
}
