package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewServiceIceberg(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceIceberg, error) {
	var err error
	var client *IcebergClient
	var settings *IcebergSettings

	if client, err = ProvideIcebergClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create iceberg client: %w", err)
	}

	if settings, err = ReadIcebergSettings(config); err != nil {
		return nil, fmt.Errorf("could not unmarshal iceberg settings: %w", err)
	}

	return &ServiceIceberg{
		logger:   logger.WithChannel("iceberg"),
		client:   client,
		settings: settings,
	}, nil
}

type ServiceIceberg struct {
	logger   log.Logger
	client   *IcebergClient
	settings *IcebergSettings
}

func (s *ServiceIceberg) ListSnapshots(ctx context.Context, logicalName string) ([]IcebergSnapshot, error) {
	snapshots, err := s.client.ListSnapshots(ctx, logicalName)
	if err != nil {
		return nil, fmt.Errorf("could not list snapshots from iceberg: %w", err)
	}

	result := make([]IcebergSnapshot, len(snapshots))
	for i, snap := range snapshots {
		summary := make(map[string]any)
		if snap.Summary != nil {
			for k, v := range snap.Summary.Properties {
				summary[k] = v
			}
		}

		operation := string(snap.Summary.Operation)
		if operation == "" {
			operation = "unknown"
		}

		result[i] = IcebergSnapshot{
			SnapshotID:   snap.SnapshotID,
			ParentID:     snap.ParentSnapshotID,
			CommittedAt:  time.UnixMilli(snap.TimestampMs),
			Operation:    operation,
			ManifestList: snap.ManifestList,
			Summary:      summary,
		}
	}

	s.logger.Info(ctx, "listed %d snapshots for table %s", len(result), logicalName)

	return result, nil
}

func (s *ServiceIceberg) ListTables(ctx context.Context) ([]string, error) {
	var err error
	var tables []table.Identifier

	if tables, err = s.client.ListTables(ctx); err != nil {
		return nil, fmt.Errorf("could not list tables from iceberg: %w", err)
	}

	result := make([]string, len(tables))
	for i, t := range tables {
		// The identifier comes as [database, table]
		result[i] = t[len(t)-1]
	}

	s.logger.Info(ctx, "listed %d tables", len(result))

	return result, nil
}

func (s *ServiceIceberg) DescribeTable(ctx context.Context, logicalName string) (*TableDescription, error) {
	desc, err := s.client.DescribeTable(ctx, logicalName)
	if err != nil {
		return nil, fmt.Errorf("could not describe table: %w", err)
	}

	s.logger.Info(ctx, "described table %s", logicalName)

	return desc, nil
}

func (s *ServiceIceberg) ListPartitions(ctx context.Context, logicalName string) ([]IcebergPartition, error) {
	var err error
	var partitionStats []IcebergPartitionStats
	var needsOptimization bool

	if partitionStats, err = s.client.ListPartitions(ctx, logicalName); err != nil {
		return nil, fmt.Errorf("could not list partitions from iceberg: %w", err)
	}

	result := make([]IcebergPartition, len(partitionStats))
	for i, stats := range partitionStats {
		if needsOptimization, err = s.partitionNeedsOptimize(stats); err != nil {
			return nil, fmt.Errorf("could not determine optimization for partition %s: %w", stats.Partition.String(), err)
		}

		result[i] = IcebergPartition{
			Partition:         stats.Partition,
			SpecID:            stats.SpecID,
			RecordCount:       stats.RecordCount,
			FileCount:         stats.FileCount,
			DataFileSizeBytes: stats.DataFileSizeBytes,
			NeedsOptimize:     needsOptimization,
			LastUpdatedAt:     time.UnixMilli(stats.LastUpdatedAt),
			LastSnapshotID:    stats.LastSnapshotID,
		}
	}

	s.logger.Info(ctx, "listed %d partitions for table %s", len(result), logicalName)

	return result, nil
}

func (s *ServiceIceberg) partitionNeedsOptimize(stats IcebergPartitionStats) (bool, error) {
	var err error
	var date *time.Time

	needsOptimize := stats.SmallFileCount > 1

	if !needsOptimize {
		return false, nil
	}

	if date, err = stats.Partition.GetDate(); err != nil {
		return false, fmt.Errorf("could not get date from partition: %w", err)
	}

	if date == nil {
		return needsOptimize, nil
	}

	age := time.Since(*date)
	if age < s.settings.NeedsOptimizeDelay {
		return false, nil
	}

	return needsOptimize, nil
}
