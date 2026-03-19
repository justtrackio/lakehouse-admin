package internal

import (
	"fmt"
	"strings"

	"github.com/justtrackio/gosoline/pkg/cfg"
)

const taskProcedureResultKey = "procedure"

type TaskSparkCallbackSettings struct {
	Enabled     bool   `cfg:"enabled"`
	BackendHost string `cfg:"backend_host"`
}

func ReadTaskSparkCallbackSettings(config cfg.Config) (*TaskSparkCallbackSettings, error) {
	settings := &TaskSparkCallbackSettings{}

	if err := config.UnmarshalKey("tasks.spark_callback", settings); err != nil {
		return nil, fmt.Errorf("could not unmarshal spark callback settings: %w", err)
	}

	settings.BackendHost = strings.TrimRight(strings.TrimSpace(settings.BackendHost), "/")

	if settings.Enabled && settings.BackendHost == "" {
		return nil, fmt.Errorf("tasks.spark_callback.backend_host is required when spark callback is enabled")
	}

	return settings, nil
}

func BuildTaskProcedureCallbackURL(host string, taskID int64) string {
	host = strings.TrimRight(strings.TrimSpace(host), "/")

	return fmt.Sprintf("%s/api/tasks/%d/callback-result", host, taskID)
}
