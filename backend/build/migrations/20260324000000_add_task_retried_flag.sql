-- +goose Up
-- +goose StatementBegin
ALTER TABLE tasks
    ADD COLUMN retried BOOLEAN NOT NULL DEFAULT FALSE AFTER status;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE tasks
    DROP COLUMN retried;
-- +goose StatementEnd
