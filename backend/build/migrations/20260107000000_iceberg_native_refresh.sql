-- +goose Up
-- +goose StatementBegin
DROP TABLE IF EXISTS snapshots;
DROP TABLE IF EXISTS partitions;

CREATE TABLE snapshots (
    `table` VARCHAR(255) NOT NULL,
    committed_at TIMESTAMP(6) NOT NULL,
    snapshot_id BIGINT NOT NULL,
    parent_id BIGINT NULL,
    operation VARCHAR(50) NOT NULL,
    manifest_list TEXT NOT NULL,
    summary JSON NOT NULL,

    PRIMARY KEY (`table`, snapshot_id)
);

CREATE TABLE partitions (
    `table` VARCHAR(255) NOT NULL,
    `partition` JSON NOT NULL,
    spec_id INT NOT NULL,
    record_count BIGINT NOT NULL,
    file_count BIGINT NOT NULL,
    total_data_file_size_in_bytes BIGINT NOT NULL,
    last_updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_updated_snapshot_id BIGINT NOT NULL
);

create index partitions_table_index on partitions (`table`);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS snapshots;
DROP TABLE IF EXISTS partitions;

CREATE TABLE snapshots (
    `table` VARCHAR(255) NOT NULL,
    committed_at TIMESTAMP(6) NOT NULL,
    snapshot_id VARCHAR(255) NOT NULL,
    parent_id VARCHAR(255),
    operation VARCHAR(50) NOT NULL,
    manifest_list TEXT NOT NULL,
    summary JSON NOT NULL,

    PRIMARY KEY (`table`, snapshot_id)
);

CREATE TABLE partitions (
    `table` VARCHAR(255) NOT NULL,
    `partition` JSON NOT NULL,
    spec_id INT NOT NULL,
    record_count BIGINT NOT NULL,
    file_count BIGINT NOT NULL,
    total_data_file_size_in_bytes BIGINT NOT NULL,
    position_delete_record_count BIGINT NOT NULL,
    position_delete_file_count BIGINT NOT NULL,
    equality_delete_record_count BIGINT NOT NULL,
    equality_delete_file_count BIGINT NOT NULL,
    last_updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_updated_snapshot_id BIGINT NOT NULL,

    partition_hash BINARY(16) AS (UNHEX(MD5(JSON_EXTRACT(`partition`, '$')))) STORED,
    PRIMARY KEY (`table`, `partition_hash`)
);
-- +goose StatementEnd
