-- +goose Up
-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS snapshots;
-- +goose StatementEnd
