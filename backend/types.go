package main

import (
	"time"

	"github.com/justtrackio/gosoline/pkg/db"
)

type Snapshot struct {
	Table        string                                  `json:"table" db:"table"`
	CommittedAt  time.Time                               `json:"committed_at" db:"committed_at"`
	SnapshotId   int64                                   `json:"snapshot_id" db:"snapshot_id"`
	ParentId     *int64                                  `json:"parent_id" db:"parent_id"`
	Operation    string                                  `json:"operation" db:"operation"`
	ManifestList string                                  `json:"manifest_list" db:"manifest_list"`
	Summary      db.JSON[map[string]any, db.NonNullable] `json:"summary" db:"summary"`
}

type sSnapshot struct {
	CommittedAt  time.Time      `json:"committed_at" db:"committed_at"`
	SnapshotId   string         `json:"snapshot_id" db:"snapshot_id"`
	ParentId     string         `json:"parent_id" db:"parent_id"`
	Operation    string         `json:"operation" db:"operation"`
	ManifestList string         `json:"manifest_list" db:"manifest_list"`
	Summary      map[string]any `json:"summary" db:"summary"`
}

type Partition struct {
	Table                    string                                  `json:"table" db:"table"`
	Partition                db.JSON[map[string]any, db.NonNullable] `json:"partition" db:"partition"`
	SpecId                   int                                     `json:"spec_id" db:"spec_id"`
	RecordCount              int64                                   `json:"record_count" db:"record_count"`
	FileCount                int64                                   `json:"file_count" db:"file_count"`
	TotalDataFileSizeInBytes int64                                   `json:"total_data_file_size_in_bytes" db:"total_data_file_size_in_bytes"`
	LastUpdatedAt            time.Time                               `json:"last_updated_at" db:"last_updated_at"`
	LastUpdatedSnapshotId    int64                                   `json:"last_updated_snapshot_id" db:"last_updated_snapshot_id"`
}

type sPartition struct {
	Partition                map[string]any `json:"partition" db:"partition"`
	SpecId                   int            `json:"spec_id" db:"spec_id"`
	RecordCount              int64          `json:"record_count" db:"record_count"`
	FileCount                int64          `json:"file_count" db:"file_count"`
	TotalDataFileSizeInBytes int64          `json:"total_data_file_size_in_bytes" db:"total_data_file_size_in_bytes"`
	LastUpdatedAt            time.Time      `json:"last_updated_at" db:"last_updated_at"`
	LastUpdatedSnapshotId    int64          `json:"last_updated_snapshot_id" db:"last_updated_snapshot_id"`
}

type TableDescription struct {
	Name       string                                    `json:"name" db:"name"`
	Columns    db.JSON[TableColumns, db.NonNullable]     `json:"columns" db:"columns"`
	Partitions db.JSON[[]TablePartition, db.NonNullable] `json:"partitions" db:"partitions"`
	UpdatedAt  time.Time                                 `json:"updated_at" db:"updated_at"`
}

type TableColumns []TableColumn

type TableColumn struct {
	Name string `json:"name" db:"name"`
	Type string `json:"type" db:"type"`
}

type TablePartition struct {
	Name     string               `json:"name" db:"name"`
	IsHidden bool                 `json:"is_hidden" db:"is_hidden"`
	Hidden   TablePartitionHidden `json:"hidden" db:"hidden"`
}

type TablePartitionHidden struct {
	Column string `json:"column" db:"column"`
	Type   string `json:"type" db:"type"`
}

type TableSummary struct {
	Name                     string           `json:"name" db:"name"`
	Partitions               []TablePartition `json:"partitions" db:"partitions"`
	SnapshotCount            int64            `json:"snapshot_count" db:"snapshot_count"`
	PartitionCount           int64            `json:"partition_count" db:"partition_count"`
	FileCount                int64            `json:"file_count" db:"file_count"`
	RecordCount              int64            `json:"record_count" db:"record_count"`
	TotalDataFileSizeInBytes int64            `json:"total_data_file_size_in_bytes" db:"total_data_file_size_in_bytes"`
	UpdatedAt                time.Time        `json:"updated_at" db:"updated_at"`
}

type MaintenanceHistory struct {
	Id           int64                                   `json:"id" db:"id"`
	Table        string                                  `json:"table" db:"table"`
	Kind         string                                  `json:"kind" db:"kind"`
	StartedAt    time.Time                               `json:"started_at" db:"started_at"`
	FinishedAt   *time.Time                              `json:"finished_at" db:"finished_at"`
	Status       string                                  `json:"status" db:"status"`
	ErrorMessage *string                                 `json:"error_message" db:"error_message"`
	Input        db.JSON[map[string]any, db.NonNullable] `json:"input" db:"input"`
	Result       db.JSON[map[string]any, db.NonNullable] `json:"result" db:"result"`
}

type sMaintenanceHistory struct {
	Id           int64          `json:"id" db:"id"`
	Table        string         `json:"table" db:"table"`
	Kind         string         `json:"kind" db:"kind"`
	StartedAt    time.Time      `json:"started_at" db:"started_at"`
	FinishedAt   *time.Time     `json:"finished_at" db:"finished_at"`
	Status       string         `json:"status" db:"status"`
	ErrorMessage *string        `json:"error_message" db:"error_message"`
	Input        map[string]any `json:"input" db:"input"`
	Result       map[string]any `json:"result" db:"result"`
}

type PaginatedMaintenanceHistory struct {
	Items []sMaintenanceHistory `json:"items"`
	Total int64                 `json:"total"`
}
