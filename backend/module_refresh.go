package main

import (
	"context"
	"fmt"
	"time"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/kernel"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewModuleRefresh(ctx context.Context, config cfg.Config, logger log.Logger) (kernel.Module, error) {
	logger = logger.WithChannel("refresh")

	var err error
	var spark *SparkClient
	var service *ServiceRefresh

	if spark, err = ProvideSparkClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create spark client: %w", err)
	}

	if service, err = NewServiceRefresh(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create spark client: %w", err)
	}

	return &ModuleRefresh{
		logger:  logger,
		spark:   spark,
		service: service,
	}, nil
}

type ModuleRefresh struct {
	logger  log.Logger
	spark   *SparkClient
	service *ServiceRefresh
}

func (m *ModuleRefresh) Run(ctx context.Context) error {
	var err error
	var tables []string
	var lastUpdatedAt time.Time

	if tables, err = m.spark.ListTables(ctx); err != nil {
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

		if _, err = m.service.RefreshTable(ctx, table); err != nil {
			return fmt.Errorf("could not refresh table %s: %w", table, err)
		}

		if _, err = m.service.RefreshPartitions(ctx, table); err != nil {
			return fmt.Errorf("could not refresh partitions for table %s: %w", table, err)
		}

		if _, err = m.service.RefreshSnapshots(ctx, table); err != nil {
			return fmt.Errorf("could not refresh snapshots for table %s: %w", table, err)
		}
	}

	return nil
}
