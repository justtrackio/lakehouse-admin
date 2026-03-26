package internal

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/justtrackio/gosoline/pkg/funk"
	"github.com/spf13/cast"
)

type IcebergSnapshot struct {
	SnapshotID   int64          `json:"snapshot_id,string"`
	ParentID     *int64         `json:"parent_id,string,omitempty"`
	CommittedAt  time.Time      `json:"committed_at"`
	Operation    string         `json:"operation"`
	ManifestList string         `json:"manifest_list"`
	Summary      map[string]any `json:"summary"`
}

type IcebergPartition struct {
	Partition         PartitionValues `json:"partition"`
	SpecID            int32           `json:"spec_id"`
	RecordCount       int64           `json:"record_count"`
	FileCount         int64           `json:"file_count"`
	DataFileSizeBytes int64           `json:"data_file_size_bytes"`
	NeedsOptimize     bool            `json:"needs_optimize"`
	LastUpdatedAt     time.Time       `json:"last_updated_at"`
	LastSnapshotID    int64           `json:"last_snapshot_id,string"`
}

type IcebergPartitionStats struct {
	Partition      PartitionValues
	RawPartition   map[int]any
	SpecID         int32
	RecordCount    int64
	Files          IcebergPartitionStatsFiles
	LastUpdatedAt  int64
	LastSnapshotID int64
}

type IcebergPartitionFileStats struct {
	SizeBytes int64
}

type IcebergPartitionStatsFiles []IcebergPartitionFileStats

func (f IcebergPartitionStatsFiles) Len() int64 {
	return int64(len(f))
}

func (f IcebergPartitionStatsFiles) Bytes() int64 {
	return funk.Reduce(f, func(value int64, file IcebergPartitionFileStats, i int) int64 {
		return value + file.SizeBytes
	}, 0)
}

type PartitionValues map[string]any

func (v PartitionValues) String() string {
	keys := funk.Keys(v)
	sort.Strings(keys)

	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%v", k, v[k])
	}

	return strings.Join(parts, ", ")
}

func (v PartitionValues) GetDate() (*time.Time, error) {
	var ok bool
	var err error
	var yearA, monthA, dayA any
	var yearS, monthS, dayS string
	var year, month, day int

	if yearA, ok = v["year"]; !ok {
		return nil, nil
	}

	if monthA, ok = v["month"]; !ok {
		return nil, nil
	}

	if dayA, ok = v["day"]; !ok {
		return nil, nil
	}

	if yearS, err = cast.ToStringE(yearA); err != nil {
		return nil, fmt.Errorf("failed to cast year to integer")
	}

	if monthS, err = cast.ToStringE(monthA); err != nil {
		return nil, fmt.Errorf("failed to cast month to integer")
	}

	if dayS, err = cast.ToStringE(dayA); err != nil {
		return nil, fmt.Errorf("failed to cast day to integer")
	}

	yearS = strings.TrimLeft(yearS, "0")
	monthS = strings.TrimLeft(monthS, "0")
	dayS = strings.TrimLeft(dayS, "0")

	if year, err = cast.ToIntE(yearS); err != nil {
		return nil, fmt.Errorf("failed to cast year to integer")
	}

	if month, err = cast.ToIntE(monthS); err != nil {
		return nil, fmt.Errorf("failed to cast month to integer")
	}

	if day, err = cast.ToIntE(dayS); err != nil {
		return nil, fmt.Errorf("failed to cast day to integer")
	}

	date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)

	return &date, nil
}
