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

type RefreshSettings struct {
	Interval time.Duration `cfg:"interval"`
}

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

	settings := &RefreshSettings{}
	if err = config.UnmarshalKey("refresh", settings); err != nil {
		return nil, fmt.Errorf("could not unmarshal refresh settings: %w", err)
	}

	return &ModuleRefresh{
		logger:    logger,
		service:   service,
		sqlClient: sqlClient,
		settings:  settings,
	}, nil
}

type ModuleRefresh struct {
	logger    log.Logger
	service   *ServiceRefresh
	sqlClient sqlc.Client
	settings  *RefreshSettings
}

func (m *ModuleRefresh) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.settings.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			m.runRefreshCycle(ctx)
		}
	}
}

func (m *ModuleRefresh) runRefreshCycle(ctx context.Context) {
	var err error
	var tables []string

	m.logger.Info(ctx, "starting periodic table refresh")

	if tables, err = m.service.ListTables(ctx); err != nil {
		m.logger.Error(ctx, "could not list tables for refresh: %s", err)
		return
	}

	for _, table := range tables {
		err = m.sqlClient.WithTx(ctx, func(cttx sqlc.Tx) error {
			if err = m.service.RefreshTableFull(cttx, table); err != nil {
				return fmt.Errorf("could not refresh table %s: %w", table, err)
			}

			return nil
		})

		if err != nil {
			m.logger.Error(ctx, "failed to refresh table %s: %s", table, err)
		}
	}

	m.logger.Info(ctx, "finished periodic table refresh")
}
