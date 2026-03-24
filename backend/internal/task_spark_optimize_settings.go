package internal

import (
	"fmt"

	"github.com/justtrackio/gosoline/pkg/cfg"
)

type TaskSparkOptimizeSettings struct {
	PartialProgressEnabled        bool `cfg:"partial_progress_enabled" default:"true"`
	PartialProgressMaxCommits     int  `cfg:"partial_progress_max_commits" default:"10"`
	MaxConcurrentFileGroupRewrite int  `cfg:"max_concurrent_file_group_rewrites" default:"5"`
}

func ReadTaskSparkOptimizeSettings(config cfg.Config) (*TaskSparkOptimizeSettings, error) {
	settings := &TaskSparkOptimizeSettings{}

	if err := config.UnmarshalKey("tasks.spark.optimize", settings); err != nil {
		return nil, fmt.Errorf("could not unmarshal spark optimize settings: %w", err)
	}

	if settings.PartialProgressMaxCommits < 1 {
		return nil, fmt.Errorf("tasks.spark.optimize.partial_progress_max_commits must be at least 1")
	}

	if settings.MaxConcurrentFileGroupRewrite < 1 {
		return nil, fmt.Errorf("tasks.spark.optimize.max_concurrent_file_group_rewrites must be at least 1")
	}

	return settings, nil
}
