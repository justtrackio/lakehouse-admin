package main

import (
	"context"
	"fmt"

	"github.com/gosoline-project/httpserver"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

type TaskConcurrencyResponse struct {
	Value int `json:"value"`
}

type SetTaskConcurrencyRequest struct {
	Value int `json:"value"`
}

func NewHandlerSettings(ctx context.Context, config cfg.Config, logger log.Logger) (*HandlerSettings, error) {
	var err error
	var serviceSettings *ServiceSettings
	var moduleTasks *ModuleTasks

	if serviceSettings, err = NewServiceSettings(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create settings service: %w", err)
	}

	if moduleTasks, err = ProvideModuleTasks(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create tasks module: %w", err)
	}

	// Get the default from config as fallback
	defaultWorkerCount, _ := config.GetInt("tasks.worker_count")
	if defaultWorkerCount < 1 {
		defaultWorkerCount = 1
	}

	return &HandlerSettings{
		serviceSettings:    serviceSettings,
		moduleTasks:        moduleTasks,
		defaultWorkerCount: defaultWorkerCount,
		logger:             logger.WithChannel("handler_settings"),
	}, nil
}

type HandlerSettings struct {
	serviceSettings    *ServiceSettings
	moduleTasks        *ModuleTasks
	defaultWorkerCount int
	logger             log.Logger
}

func (h *HandlerSettings) GetTaskConcurrency(ctx context.Context) (httpserver.Response, error) {
	var err error
	var value int

	if value, err = h.serviceSettings.GetIntSetting(ctx, "task_concurrency", h.defaultWorkerCount); err != nil {
		return nil, fmt.Errorf("failed to get task concurrency setting: %w", err)
	}

	return httpserver.NewJsonResponse(&TaskConcurrencyResponse{
		Value: value,
	}), nil
}

func (h *HandlerSettings) SetTaskConcurrency(ctx context.Context, input *SetTaskConcurrencyRequest) (httpserver.Response, error) {
	if input.Value < 1 {
		return nil, fmt.Errorf("task concurrency must be at least 1")
	}

	if err := h.serviceSettings.SetSetting(ctx, "task_concurrency", fmt.Sprintf("%d", input.Value)); err != nil {
		return nil, fmt.Errorf("failed to set task concurrency: %w", err)
	}

	h.moduleTasks.SetWorkerCount(input.Value)
	h.logger.Info(ctx, "updated task concurrency to %d", input.Value)

	return httpserver.NewJsonResponse(&TaskConcurrencyResponse{
		Value: input.Value,
	}), nil
}
