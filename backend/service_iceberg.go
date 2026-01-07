package main

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

	if client, err = ProvideIcebergClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create iceberg client: %w", err)
	}

	return &ServiceIceberg{
		logger: logger.WithChannel("iceberg"),
		client: client,
	}, nil
}

type ServiceIceberg struct {
	logger log.Logger
	client *IcebergClient
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
	partitionStats, err := s.client.ListPartitions(ctx, logicalName)
	if err != nil {
		return nil, fmt.Errorf("could not list partitions from iceberg: %w", err)
	}

	result := make([]IcebergPartition, len(partitionStats))
	for i, stats := range partitionStats {
		result[i] = IcebergPartition{
			Partition:         stats.Partition,
			SpecID:            stats.SpecID,
			RecordCount:       stats.RecordCount,
			FileCount:         stats.FileCount,
			DataFileSizeBytes: stats.DataFileSizeBytes,
			LastUpdatedAt:     time.UnixMilli(stats.LastUpdatedAt),
			LastSnapshotID:    stats.LastSnapshotID,
		}
	}

	s.logger.Info(ctx, "listed %d partitions for table %s", len(result), logicalName)

	return result, nil
}
