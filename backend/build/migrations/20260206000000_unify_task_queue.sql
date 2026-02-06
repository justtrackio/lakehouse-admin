-- +goose Up
-- +goose StatementBegin
CREATE TABLE tasks (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    `table` VARCHAR(255) NOT NULL,
    kind VARCHAR(50) NOT NULL,
    started_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    picked_up_at TIMESTAMP(6) NULL,
    finished_at TIMESTAMP(6) NULL,
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    input JSON NOT NULL,
    result JSON NOT NULL,
    
    INDEX idx_table_started (`table`, started_at),
    INDEX idx_status_started (status, started_at)
);

DROP TABLE IF EXISTS maintenance_tasks;
DROP TABLE IF EXISTS analyzer_jobs;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- Recreate the original tables
CREATE TABLE maintenance_tasks (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    `table` VARCHAR(255) NOT NULL,
    kind VARCHAR(50) NOT NULL,
    started_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    picked_up_at TIMESTAMP(6) NULL,
    finished_at TIMESTAMP(6) NULL,
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    input JSON NOT NULL,
    result JSON NOT NULL,
    
    INDEX idx_table_started (`table`, started_at),
    INDEX idx_status_started (status, started_at)
);

CREATE TABLE analyzer_jobs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    `table` VARCHAR(255) NOT NULL,
    kind VARCHAR(50) NOT NULL,
    started_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    picked_up_at TIMESTAMP(6) NULL,
    finished_at TIMESTAMP(6) NULL,
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    input JSON NOT NULL,
    result JSON NOT NULL,
    
    INDEX idx_table_started (`table`, started_at),
    INDEX idx_status_started (status, started_at)
);

DROP TABLE IF EXISTS tasks;
-- +goose StatementEnd
