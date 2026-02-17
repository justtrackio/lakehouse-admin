package internal

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type ServiceSettings struct {
	logger    log.Logger
	sqlClient sqlc.Client
}

type Setting struct {
	Key       string `db:"key"`
	Value     string `db:"value"`
	UpdatedAt string `db:"updated_at"`
}

func NewServiceSettings(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceSettings, error) {
	var err error
	var sqlClient sqlc.Client

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlc client: %w", err)
	}

	return &ServiceSettings{
		logger:    logger.WithChannel("settings"),
		sqlClient: sqlClient,
	}, nil
}

// GetSetting retrieves a setting value by key. Returns sql.ErrNoRows if not found.
func (s *ServiceSettings) GetSetting(ctx context.Context, key string) (string, error) {
	var setting Setting
	var err error

	query := s.sqlClient.Q().From("settings").Where(sqlc.Eq{"key": key})
	if err = query.Get(ctx, &setting); err != nil {
		return "", fmt.Errorf("could not get setting %s: %w", key, err)
	}

	return setting.Value, nil
}

// SetSetting upserts a setting (creates or updates).
func (s *ServiceSettings) SetSetting(ctx context.Context, key string, value string) error {
	var err error

	// Use INSERT ... ON DUPLICATE KEY UPDATE for upsert
	rawSQL := "INSERT INTO settings (`key`, `value`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `value` = ?, updated_at = CURRENT_TIMESTAMP(6)"

	if _, err = s.sqlClient.Exec(ctx, rawSQL, key, value, value); err != nil {
		return fmt.Errorf("could not set setting %s: %w", key, err)
	}

	s.logger.Info(ctx, "setting updated: %s = %s", key, value)

	return nil
}

// GetIntSetting retrieves an integer setting with a default fallback.
func (s *ServiceSettings) GetIntSetting(ctx context.Context, key string, defaultValue int) (int, error) {
	value, err := s.GetSetting(ctx, key)
	if err != nil {
		// Check if it's a "not found" error - in that case return default
		errStr := err.Error()
		if strings.Contains(errStr, "no rows in result set") {
			return defaultValue, nil
		}

		return 0, err
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("could not parse setting %s as int: %w", key, err)
	}

	return intValue, nil
}
