package internal

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewServiceIceberg(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceIceberg, error) {
	var err error
	var client *IcebergClient
	var settings *IcebergSettings
	var serviceSettings *ServiceSettings

	if client, err = ProvideIcebergClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create iceberg client: %w", err)
	}

	if serviceSettings, err = NewServiceSettings(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create settings service: %w", err)
	}

	if settings, err = ReadIcebergSettings(config); err != nil {
		return nil, fmt.Errorf("could not unmarshal iceberg settings: %w", err)
	}

	return &ServiceIceberg{
		logger:          logger.WithChannel("iceberg"),
		client:          client,
		settings:        settings,
		serviceSettings: serviceSettings,
	}, nil
}

type ServiceIceberg struct {
	logger          log.Logger
	client          *IcebergClient
	settings        *IcebergSettings
	serviceSettings *ServiceSettings
}

func (s *ServiceIceberg) ListSnapshots(ctx context.Context, database string, logicalName string) ([]IcebergSnapshot, error) {
	snapshots, err := s.client.ListSnapshots(ctx, database, logicalName)
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

	s.logger.Info(ctx, "listed %d snapshots for table %s.%s", len(result), database, logicalName)

	return result, nil
}

func (s *ServiceIceberg) ListTables(ctx context.Context, database string) ([]CatalogTable, error) {
	var err error
	var tables []table.Identifier

	if tables, err = s.client.ListTables(ctx, database); err != nil {
		return nil, fmt.Errorf("could not list tables from iceberg: %w", err)
	}

	result := make([]CatalogTable, len(tables))
	for i, t := range tables {
		if len(t) < 2 {
			return nil, fmt.Errorf("unexpected table identifier: %v", t)
		}

		result[i] = CatalogTable{
			Database: t[len(t)-2],
			Name:     t[len(t)-1],
		}
	}

	s.logger.Info(ctx, "listed %d tables for database %s", len(result), database)

	return result, nil
}

func (s *ServiceIceberg) DescribeTable(ctx context.Context, database string, logicalName string) (*TableDescription, error) {
	desc, err := s.client.DescribeTable(ctx, database, logicalName)
	if err != nil {
		return nil, fmt.Errorf("could not describe table: %w", err)
	}

	s.logger.Info(ctx, "described table %s.%s", database, logicalName)

	return desc, nil
}

func (s *ServiceIceberg) ListPartitions(ctx context.Context, database string, logicalName string) ([]IcebergPartition, error) {
	var err error
	var partitionStats []IcebergPartitionStats
	var needsOptimization bool
	var smallFileThresholdBytes int64
	var smallFileMinCount int
	var smallFileMinSharePct int

	if partitionStats, err = s.client.ListPartitions(ctx, database, logicalName); err != nil {
		return nil, fmt.Errorf("could not list partitions from iceberg: %w", err)
	}

	if smallFileThresholdBytes, err = s.serviceSettings.GetInt64Setting(ctx, settingKeySmallFileThresholdBytes, defaultSmallFileThresholdBytes); err != nil {
		return nil, fmt.Errorf("could not load iceberg small file threshold bytes: %w", err)
	}

	if smallFileMinCount, err = s.serviceSettings.GetIntSetting(ctx, settingKeySmallFileMinCount, defaultSmallFileMinCount); err != nil {
		return nil, fmt.Errorf("could not load iceberg small file minimum count: %w", err)
	}

	if smallFileMinCount < 1 {
		return nil, fmt.Errorf("iceberg small file minimum count must be at least 1")
	}

	if smallFileMinSharePct, err = s.serviceSettings.GetIntSetting(ctx, settingKeySmallFileMinSharePct, defaultSmallFileMinSharePct); err != nil {
		return nil, fmt.Errorf("could not load iceberg small file minimum share percent: %w", err)
	}

	if smallFileMinSharePct < 0 || smallFileMinSharePct > 100 {
		return nil, fmt.Errorf("iceberg small file minimum share percent must be between 0 and 100")
	}

	result := make([]IcebergPartition, len(partitionStats))
	for i, stats := range partitionStats {
		if needsOptimization, err = s.partitionNeedsOptimize(stats, smallFileThresholdBytes, int64(smallFileMinCount), int64(smallFileMinSharePct)); err != nil {
			return nil, fmt.Errorf("could not determine optimization for partition %s: %w", stats.Partition.String(), err)
		}

		result[i] = IcebergPartition{
			Partition:         stats.Partition,
			SpecID:            stats.SpecID,
			RecordCount:       stats.RecordCount,
			FileCount:         stats.Files.Len(),
			DataFileSizeBytes: stats.Files.Bytes(),
			NeedsOptimize:     needsOptimization,
			LastUpdatedAt:     time.UnixMilli(stats.LastUpdatedAt),
			LastSnapshotID:    stats.LastSnapshotID,
		}
	}

	s.logger.Info(ctx, "listed %d partitions for table %s.%s", len(result), database, logicalName)

	return result, nil
}

func (s *ServiceIceberg) ListDatabases(ctx context.Context) ([]CatalogDatabase, error) {
	databases, err := s.client.ListDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not list databases from iceberg: %w", err)
	}

	result := make([]CatalogDatabase, 0, len(databases))
	for _, database := range databases {
		tables, err := s.client.ListTables(ctx, database)
		if err != nil {
			return nil, fmt.Errorf("could not list tables for database %s: %w", database, err)
		}

		if len(tables) == 0 {
			continue
		}

		result = append(result, CatalogDatabase{
			Name:      database,
			IsDefault: database == s.settings.DefaultDatabase,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

func (s *ServiceIceberg) partitionNeedsOptimize(stats IcebergPartitionStats, smallFileThresholdBytes int64, smallFileMinCount int64, smallFileMinSharePct int64) (bool, error) {
	var err error
	var date *time.Time
	var smallFileCount int64

	totalFileCount := stats.Files.Len()
	if totalFileCount == 0 {
		return false, nil
	}

	for _, file := range stats.Files {
		if file.SizeBytes < smallFileThresholdBytes {
			smallFileCount++
		}
	}

	needsOptimize := smallFileCount >= smallFileMinCount && smallFileCount*100 >= totalFileCount*smallFileMinSharePct

	if !needsOptimize {
		return false, nil
	}

	if date, err = stats.Partition.GetDate(); err != nil {
		return false, fmt.Errorf("could not get date from partition: %w", err)
	}

	if date == nil {
		return needsOptimize, nil
	}

	if !canOptimizePartitionDate(*date, time.Now().UTC(), s.settings.NeedsOptimizeDelay) {
		return false, nil
	}

	return needsOptimize, nil
}

func latestOptimizablePartitionDate(now time.Time, delay time.Duration) time.Time {
	now = now.Add(-delay)

	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}

func canOptimizePartitionDate(date time.Time, now time.Time, delay time.Duration) bool {
	date = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)

	return !date.After(latestOptimizablePartitionDate(now, delay))
}
