-- +goose Up
-- +goose StatementBegin
CREATE TABLE maintenance_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    `table` VARCHAR(255) NOT NULL,
    kind VARCHAR(50) NOT NULL,
    started_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    finished_at TIMESTAMP(6) NULL,
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    input JSON NOT NULL,
    result JSON NOT NULL,
    
    INDEX idx_table_started (`table`, started_at),
    INDEX idx_started (started_at)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS maintenance_history;
-- +goose StatementEnd
