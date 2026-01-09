package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/kernel"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewModuleRefresh(ctx context.Context, config cfg.Config, logger log.Logger) (kernel.Module, error) {
	logger = logger.WithChannel("refresh")

	var err error
	var service *ServiceRefresh
	var sqlClient sqlc.Client

	if service, err = NewServiceRefresh(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create refresh service: %w", err)
	}

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlc client: %w", err)
	}

	return &ModuleRefresh{
		logger:    logger,
		service:   service,
		sqlClient: sqlClient,
	}, nil
}

type ModuleRefresh struct {
	logger    log.Logger
	service   *ServiceRefresh
	sqlClient sqlc.Client
}

func (m *ModuleRefresh) Run(ctx context.Context) error {
	var err error
	var tables []string
	var lastUpdatedAt time.Time

	if tables, err = m.service.ListTables(ctx); err != nil {
		return fmt.Errorf("could not list tables: %w", err)
	}

	for _, table := range tables {
		if lastUpdatedAt, err = m.service.LastUpdatedAt(ctx, table); err != nil {
			return fmt.Errorf("could not get table %s from db: %w", table, err)
		}

		if time.Since(lastUpdatedAt) < 10*time.Minute {
			m.logger.Info(ctx, "skipping refresh for table %s, last updated at %s", table, lastUpdatedAt.Format(time.RFC3339))

			continue
		}

		err = m.sqlClient.WithTx(ctx, func(cttx sqlc.Tx) error {
			if err = m.service.RefreshTableFull(cttx, table); err != nil {
				return fmt.Errorf("could not refresh table %s: %w", table, err)
			}

			return nil
		})
	}

	return nil
}
