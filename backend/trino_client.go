package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/exec"
	"github.com/justtrackio/gosoline/pkg/log"
	_ "github.com/trinodb/trino-go-client/trino"
)

type TrinoSettings struct {
	DSN string `cfg:"dsn"`
}

type trinoCtxKey struct{}

func ProvideTrinoClient(ctx context.Context, config cfg.Config, logger log.Logger) (*TrinoClient, error) {
	return appctx.Provide(ctx, trinoCtxKey{}, func() (*TrinoClient, error) {
		var err error
		var db *sqlx.DB
		var backoffSettings exec.BackoffSettings

		settings := &TrinoSettings{}
		if err = config.UnmarshalKey("trino", settings); err != nil {
			return nil, fmt.Errorf("could not unmarshal trino settings: %w", err)
		}

		if db, err = sqlx.Open("trino", settings.DSN); err != nil {
			return nil, fmt.Errorf("could not connect to database: %w", err)
		}

		if backoffSettings, err = exec.ReadBackoffSettings(config); err != nil {
			return nil, fmt.Errorf("could not read backoff settings: %w", err)
		}

		checks := []exec.ErrorChecker{
			exec.CheckConnectionError,
			func(_ any, err error) exec.ErrorType {
				if strings.Contains(err.Error(), "query failed") {
					return exec.ErrorTypeRetryable
				}

				return exec.ErrorTypePermanent
			},
		}
		executor := exec.NewExecutor(logger, &exec.ExecutableResource{Type: "trino", Name: "default"}, &backoffSettings, checks)

		return &TrinoClient{
			db:   db,
			exec: executor,
		}, nil
	})
}

type TrinoClient struct {
	db   *sqlx.DB
	exec exec.Executor
}

func (c *TrinoClient) ListPartitions(ctx context.Context, table string) ([]sPartition, error) {
	result := make([]sPartition, 0)

	query := fmt.Sprintf(`
		SELECT
			partition,
			spec_id,
			record_count,
			file_count,
			total_data_file_size_in_bytes,
			last_updated_at,
			last_updated_snapshot_id
		FROM "%s$partitions"
	`, table)

	if err := c.db.Select(&result, query); err != nil {
		return nil, fmt.Errorf("could not list partitions: %w", err)
	}

	return result, nil
}

func (s *TrinoClient) Query(ctx context.Context, query string, args ...any) (*sqlx.Rows, error) {
	res, err := s.exec.Execute(ctx, func(ctx context.Context) (any, error) {
		return s.db.QueryxContext(ctx, query, args...)
	})

	if err != nil {
		return nil, err
	}

	return res.(*sqlx.Rows), nil
}
