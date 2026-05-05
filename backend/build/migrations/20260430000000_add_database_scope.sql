-- +goose Up
-- +goose StatementBegin
ALTER TABLE `tables`
    ADD COLUMN `database` VARCHAR(255) NULL FIRST;

UPDATE `tables`
SET `database` = 'main'
WHERE `database` IS NULL;

ALTER TABLE `tables`
    MODIFY COLUMN `database` VARCHAR(255) NOT NULL,
    DROP INDEX `name_pk`,
    ADD UNIQUE KEY `database_name_pk` (`database`, `name`);

ALTER TABLE `partitions`
    ADD COLUMN `database` VARCHAR(255) NULL FIRST;

UPDATE `partitions`
SET `database` = 'main'
WHERE `database` IS NULL;

ALTER TABLE `partitions`
    MODIFY COLUMN `database` VARCHAR(255) NOT NULL,
    DROP INDEX `partitions_table_index`,
    ADD INDEX `partitions_database_table_index` (`database`, `table`);

ALTER TABLE `snapshots`
    ADD COLUMN `database` VARCHAR(255) NULL FIRST;

UPDATE `snapshots`
SET `database` = 'main'
WHERE `database` IS NULL;

ALTER TABLE `snapshots`
    DROP PRIMARY KEY,
    MODIFY COLUMN `database` VARCHAR(255) NOT NULL,
    ADD PRIMARY KEY (`database`, `table`, `snapshot_id`);

ALTER TABLE `tasks`
    ADD COLUMN `database` VARCHAR(255) NULL AFTER `id`;

UPDATE `tasks`
SET `database` = 'main'
WHERE `database` IS NULL;

ALTER TABLE `tasks`
    MODIFY COLUMN `database` VARCHAR(255) NOT NULL,
    DROP INDEX `idx_table_started`,
    ADD INDEX `idx_database_table_started` (`database`, `table`, `started_at`),
    DROP INDEX `idx_kind_engine_status`,
    ADD INDEX `idx_database_kind_engine_status` (`database`, `kind`, `engine`, `status`);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE `tasks`
    DROP INDEX `idx_database_table_started`,
    ADD INDEX `idx_table_started` (`table`, `started_at`),
    DROP INDEX `idx_database_kind_engine_status`,
    ADD INDEX `idx_kind_engine_status` (`kind`, `engine`, `status`),
    DROP COLUMN `database`;

ALTER TABLE `snapshots`
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (`table`, `snapshot_id`),
    DROP COLUMN `database`;

ALTER TABLE `partitions`
    DROP INDEX `partitions_database_table_index`,
    ADD INDEX `partitions_table_index` (`table`),
    DROP COLUMN `database`;

ALTER TABLE `tables`
    DROP INDEX `database_name_pk`,
    ADD UNIQUE KEY `name_pk` (`name`),
    DROP COLUMN `database`;
-- +goose StatementEnd
