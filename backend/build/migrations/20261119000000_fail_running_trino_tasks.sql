-- +goose Up
-- +goose StatementBegin
-- The Trino executor is gone. Running Trino tasks can no longer complete and would
-- hold task slots forever. Queued ones switch to the current engine when claimed.
UPDATE `tasks`
SET `status` = 'error',
    `error_message` = 'the trino engine was removed before this task completed; retry it to run on spark',
    `finished_at` = CURRENT_TIMESTAMP(6)
WHERE `engine` = 'trino' AND `status` = 'running';
-- +goose StatementEnd

-- +goose Down
SELECT 1;
