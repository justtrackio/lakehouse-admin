-- +goose Up
ALTER TABLE partitions ADD COLUMN needs_optimize BOOLEAN NOT NULL DEFAULT FALSE;

-- +goose Down
ALTER TABLE partitions DROP COLUMN needs_optimize;
