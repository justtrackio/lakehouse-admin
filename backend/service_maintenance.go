package main

import (
	"context"
	"fmt"
	"time"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type ExpireSnapshotsResult struct {
	DeletedDataFilesCount           int64 `json:"deleted_data_files_count" db:"deleted_data_files_count"`
	DeletedPositionDeleteFilesCount int64 `json:"deleted_position_delete_files_count" db:"deleted_position_delete_files_count"`
	DeletedEqualityDeleteFilesCount int64 `json:"deleted_equality_delete_files_count" db:"deleted_equality_delete_files_count"`
	DeletedManifestFilesCount       int64 `json:"deleted_manifest_files_count" db:"deleted_manifest_files_count"`
	DeletedManifestListsCount       int64 `json:"deleted_manifest_lists_count" db:"deleted_manifest_lists_count"`
	DeletedStatisticsFilesCount     int64 `json:"deleted_statistics_files_count" db:"deleted_statistics_files_count"`
}

func NewServiceMaintenance(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceMaintenance, error) {
	var err error
	var spark *SparkClient

	if spark, err = ProvideSparkClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create spark client: %w", err)
	}

	return &ServiceMaintenance{
		logger: logger.WithChannel("maintenance"),
		spark:  spark,
	}, nil
}

type ServiceMaintenance struct {
	logger log.Logger
	spark  *SparkClient
}

func (s *ServiceMaintenance) ExpireSnapshots(ctx context.Context, table string, olderThan DateTime, retainLast int) (*ExpireSnapshotsResult, error) {
	sql := fmt.Sprintf("CALL lakehouse.system.expire_snapshots(table => 'main.%s', older_than =>  TIMESTAMP '%s', retain_last => %d, clean_expired_metadata => true, max_concurrent_deletes => 10);", table, olderThan.Format(time.DateTime), retainLast)
	result := make([]ExpireSnapshotsResult, 0)

	if err := s.spark.Call(ctx, sql, &result); err != nil {
		return nil, fmt.Errorf("could not expire snapshots for table %s: %w", table, err)
	}

	if len(result) != 1 {
		return nil, fmt.Errorf("unexpected number of results from expire snapshots for table %s: %d", table, len(result))
	}

	return &result[0], nil
}
