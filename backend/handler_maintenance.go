package main

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type ExpireSnapshotsInput struct {
	Table      string   `uri:"table"`
	OlderThan  DateTime `json:"older_than"`
	RetainLast int      `json:"retain_last"`
}

func NewHandlerMaintenance(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerMaintenance, error) {
	var err error
	var service *ServiceMaintenance

	if service, err = NewServiceMaintenance(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create spark client: %w", err)
	}

	return &HandlerMaintenance{
		service: service,
	}, nil
}

type HandlerMaintenance struct {
	service *ServiceMaintenance
}

func (h *HandlerMaintenance) ExpireSnapshots(ctx context.Context, input *ExpireSnapshotsInput) (httpserver.Response, error) {
	var err error
	var result *ExpireSnapshotsResult

	if result, err = h.service.ExpireSnapshots(ctx, input.Table, input.OlderThan, input.RetainLast); err != nil {
		return nil, fmt.Errorf("could not maintenance all tables: %w", err)
	}

	return httpserver.NewJsonResponse(result), nil
}
