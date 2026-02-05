-- +goose Up
-- +goose StatementBegin
ALTER TABLE maintenance_history RENAME TO maintenance_tasks;

ALTER TABLE maintenance_tasks
ADD COLUMN picked_up_at TIMESTAMP(6) NULL AFTER started_at;

-- Index for efficient worker queue claiming (find oldest queued task)
CREATE INDEX idx_status_started ON maintenance_tasks (status, started_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX idx_status_started ON maintenance_tasks;

ALTER TABLE maintenance_tasks
DROP COLUMN picked_up_at;

ALTER TABLE maintenance_tasks RENAME TO maintenance_history;
-- +goose StatementEnd
