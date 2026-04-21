-- +goose Up
-- +goose StatementBegin
ALTER TABLE `tables`
    ADD COLUMN `current_snapshot_id` BIGINT NULL AFTER `partitions`;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE `tables`
    DROP COLUMN `current_snapshot_id`;
-- +goose StatementEnd
