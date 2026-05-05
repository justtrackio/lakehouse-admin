package internal

import (
	"fmt"
	"strings"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/funk"
)

type SparkSettings struct {
	Callback SparkCallbackSettings `cfg:"callback"`
	Optimize SparkOptimizeSettings `cfg:"optimize"`
	PodSpec  SparkPodSpecSettings  `cfg:"pod_spec"`
}

type SparkCallbackSettings struct {
	Enabled     bool   `cfg:"enabled"`
	BackendHost string `cfg:"backend_host"`
}

type SparkOptimizeSettings struct {
	PartialProgressEnabled        bool `cfg:"partial_progress_enabled" default:"true"`
	PartialProgressMaxCommits     int  `cfg:"partial_progress_max_commits" default:"10"`
	MaxConcurrentFileGroupRewrite int  `cfg:"max_concurrent_file_group_rewrites" default:"5"`
}

type SparkPodSpecSettings struct {
	Annotations  map[string]string            `cfg:"annotations"`
	NodeSelector map[string]string            `cfg:"node_selector"`
	Tolerations  []SparkApplicationToleration `cfg:"tolerations"`
}

func ReadSparkSettings(config cfg.Config) (*SparkSettings, error) {
	settings := &SparkSettings{}
	if err := config.UnmarshalKey("tasks.spark", settings); err != nil {
		return nil, fmt.Errorf("could not unmarshal spark callback settings: %w", err)
	}

	settings.Callback.BackendHost = strings.TrimRight(strings.TrimSpace(settings.Callback.BackendHost), "/")

	if settings.Callback.Enabled && settings.Callback.BackendHost == "" {
		return nil, fmt.Errorf("callback.backend_host is required when spark callback is enabled")
	}

	if settings.Optimize.PartialProgressMaxCommits < 1 {
		return nil, fmt.Errorf("optimize.partial_progress_max_commits must be at least 1")
	}

	if settings.Optimize.MaxConcurrentFileGroupRewrite < 1 {
		return nil, fmt.Errorf("ptimize.max_concurrent_file_group_rewrites must be at least 1")
	}

	settings.PodSpec.Annotations = funk.MapKeys(settings.PodSpec.Annotations, func(key string) string {
		return strings.ReplaceAll(key, "\\.", ".")
	})

	settings.PodSpec.NodeSelector = funk.MapKeys(settings.PodSpec.NodeSelector, func(key string) string {
		return strings.ReplaceAll(key, "\\.", ".")
	})

	for i := range settings.PodSpec.Tolerations {
		settings.PodSpec.Tolerations[i].Key = strings.ReplaceAll(settings.PodSpec.Tolerations[i].Key, "\\.", ".")
	}

	return settings, nil
}

func BuildTaskProcedureCallbackURL(host string, taskID int64) string {
	host = strings.TrimRight(strings.TrimSpace(host), "/")

	return fmt.Sprintf("%s/api/tasks/callback/%d/result", host, taskID)
}
