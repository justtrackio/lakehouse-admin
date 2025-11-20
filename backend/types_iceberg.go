package main

import (
	"time"
)

type IcebergSnapshot struct {
	SnapshotID   int64          `json:"snapshot_id"`
	ParentID     *int64         `json:"parent_id,omitempty"`
	CommittedAt  time.Time      `json:"committed_at"`
	Operation    string         `json:"operation"`
	ManifestList string         `json:"manifest_list"`
	Summary      map[string]any `json:"summary"`
}

type IcebergPartition struct {
	Partition         map[string]any `json:"partition"`
	SpecID            int32          `json:"spec_id"`
	RecordCount       int64          `json:"record_count"`
	FileCount         int64          `json:"file_count"`
	DataFileSizeBytes int64          `json:"data_file_size_bytes"`
	LastUpdatedAt     time.Time      `json:"last_updated_at"`
	LastSnapshotID    int64          `json:"last_snapshot_id"`
}
