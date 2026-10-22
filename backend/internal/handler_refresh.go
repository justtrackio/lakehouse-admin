package internal

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/gosoline-project/sqlh"
	"github.com/gosoline-project/sqlr"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewHandlerRefresh(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerRefresh, error) {
	var err error
	var service *ServiceRefresh
	var txRunner *sqlh.TxRunner

	if service, err = NewServiceRefresh(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create spark client: %w", err)
	}

	if txRunner, err = sqlh.NewTxRunner(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create transaction runner: %w", err)
	}

	return &HandlerRefresh{
		service:  service,
		txRunner: txRunner,
	}, nil
}

type HandlerRefresh struct {
	service  *ServiceRefresh
	txRunner *sqlh.TxRunner
}

func (h *HandlerRefresh) RefreshTables(ctx context.Context) (httpserver.Response, error) {
	return h.runInTransaction(ctx, func(tx sqlr.TTx) (httpserver.Response, error) {
		if _, err := h.service.RefreshAllTables(tx); err != nil {
			return nil, fmt.Errorf("could not refresh all tables: %w", err)
		}

		return httpserver.NewJsonResponse(map[string]string{"status": statusOK}), nil
	})
}

func (h *HandlerRefresh) RefreshTable(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	return h.runInTransaction(ctx, func(tx sqlr.TTx) (httpserver.Response, error) {
		if err := h.service.RefreshTableFull(tx, input.Database, input.Table); err != nil {
			return nil, fmt.Errorf("could not refresh table: %w", err)
		}

		return httpserver.NewJsonResponse(map[string]string{"status": statusOK}), nil
	})
}

func (h *HandlerRefresh) RefreshPartitions(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	return h.runInTransaction(ctx, func(tx sqlr.TTx) (httpserver.Response, error) {
		var partitions []Partition
		var err error
		if partitions, err = h.service.RefreshPartitions(tx, input.Database, input.Table); err != nil {
			return nil, fmt.Errorf("could not list snapshots: %w", err)
		}

		return httpserver.NewJsonResponse(partitions), nil
	})
}

func (h *HandlerRefresh) RefreshSnapshots(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
	return h.runInTransaction(ctx, func(tx sqlr.TTx) (httpserver.Response, error) {
		var snapshots []Snapshot
		var err error
		if snapshots, err = h.service.RefreshSnapshots(tx, input.Database, input.Table); err != nil {
			return nil, fmt.Errorf("could not refresh snapshots: %w", err)
		}

		return httpserver.NewJsonResponse(snapshots), nil
	})
}

func (h *HandlerRefresh) RefreshFull(ctx context.Context) (httpserver.Response, error) {
	return h.runInTransaction(ctx, func(tx sqlr.TTx) (httpserver.Response, error) {
		if _, err := h.service.RefreshFull(tx); err != nil {
			return nil, fmt.Errorf("could not complete full refresh: %w", err)
		}

		return httpserver.NewJsonResponse(map[string]string{"status": statusOK}), nil
	})
}

func (h *HandlerRefresh) RefreshDatabase(ctx context.Context, input *DatabaseInput) (httpserver.Response, error) {
	return h.runInTransaction(ctx, func(tx sqlr.TTx) (httpserver.Response, error) {
		if _, err := h.service.RefreshDatabase(tx, input.Database); err != nil {
			return nil, fmt.Errorf("could not complete database refresh for %s: %w", input.Database, err)
		}

		return httpserver.NewJsonResponse(map[string]string{"status": statusOK}), nil
	})
}

func (h *HandlerRefresh) runInTransaction(ctx context.Context, operation func(sqlr.TTx) (httpserver.Response, error)) (httpserver.Response, error) {
	var response httpserver.Response

	err := h.txRunner.Run(ctx, func(tx sqlr.TTx) error {
		var err error
		response, err = operation(tx)

		return err
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}
