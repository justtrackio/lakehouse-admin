-- +goose Up
-- +goose StatementBegin
ALTER TABLE `tables`
    ADD COLUMN `optimize_disabled` BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN `expire_snapshots_disabled` BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN `remove_orphan_files_disabled` BOOLEAN NOT NULL DEFAULT FALSE;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE `tables`
    DROP COLUMN `remove_orphan_files_disabled`,
    DROP COLUMN `expire_snapshots_disabled`,
    DROP COLUMN `optimize_disabled`;
-- +goose StatementEnd
