-- +goose Up
-- +goose StatementBegin
CREATE TABLE `tables` (
    `name`       varchar(255) NOT NULL,
    `columns`    json NOT NULL,
    `partitions` json NOT NULL,
    `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `name_pk` (`name`)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
drop table tables;
-- +goose StatementEnd
