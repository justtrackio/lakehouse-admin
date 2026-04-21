package internal

import (
	"context"
	"fmt"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/kernel"
	"github.com/justtrackio/gosoline/pkg/log"
)

type RefreshSettings struct {
	Enabled bool   `cfg:"enabled"`
	Cron    string `cfg:"cron"`
}

func ReadRefreshSettings(config cfg.Config) (*RefreshSettings, error) {
	settings := &RefreshSettings{}
	if err := config.UnmarshalKey("refresh", settings); err != nil {
		return nil, fmt.Errorf("could not unmarshal refresh settings: %w", err)
	}

	if !settings.Enabled {
		return settings, nil
	}

	if _, err := parseStandardCronSchedule(settings.Cron); err != nil {
		return nil, fmt.Errorf("invalid refresh cron expression: %w", err)
	}

	return settings, nil
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

	var settings *RefreshSettings
	if settings, err = ReadRefreshSettings(config); err != nil {
		return nil, fmt.Errorf("could not read refresh settings: %w", err)
	}

	return &ModuleRefresh{
		logger:    logger,
		service:   service,
		sqlClient: sqlClient,
		settings:  settings,
	}, nil
}

type ModuleRefresh struct {
	kernel.ServiceStage
	kernel.BackgroundModule

	logger    log.Logger
	service   *ServiceRefresh
	sqlClient sqlc.Client
	settings  *RefreshSettings
}

func (m *ModuleRefresh) Run(ctx context.Context) error {
	if !m.settings.Enabled {
		return nil
	}

	return runCronLoop(ctx, m.logger, "table refresh", m.settings.Cron, m.runRefreshCycle)
}

func (m *ModuleRefresh) runRefreshCycle(ctx context.Context) {
	m.logger.Info(ctx, "starting scheduled table refresh")

	err := m.sqlClient.WithTx(ctx, func(cttx sqlc.Tx) error {
		if _, err := m.service.RefreshFull(cttx); err != nil {
			return fmt.Errorf("could not complete scheduled full refresh: %w", err)
		}

		return nil
	})
	if err != nil {
		m.logger.Error(ctx, "failed scheduled table refresh: %s", err)

		return
	}

	m.logger.Info(ctx, "finished scheduled table refresh")
}
