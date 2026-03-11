-- +goose Up
-- +goose StatementBegin
ALTER TABLE tasks
    ADD COLUMN engine VARCHAR(50) NOT NULL DEFAULT 'trino' AFTER kind;

CREATE INDEX idx_kind_engine_status ON tasks (kind, engine, status);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX idx_kind_engine_status ON tasks;

ALTER TABLE tasks
    DROP COLUMN engine;
-- +goose StatementEnd
