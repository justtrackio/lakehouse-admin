package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/robfig/cron/v3"
)

var standardCronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

func parseStandardCronSchedule(spec string) (cron.Schedule, error) {
	schedule, err := standardCronParser.Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("invalid 5-field cron expression %q: %w", spec, err)
	}

	return schedule, nil
}

func runCronLoop(ctx context.Context, logger log.Logger, jobName string, spec string, run func(context.Context)) error {
	schedule, err := parseStandardCronSchedule(spec)
	if err != nil {
		return fmt.Errorf("could not parse %s cron expression: %w", jobName, err)
	}

	logger.Info(ctx, "starting %s scheduler with cron %q", jobName, spec)

	for {
		next := schedule.Next(time.Now().UTC())
		logger.Info(ctx, "next %s run scheduled for %s", jobName, next.Format(time.RFC3339))

		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			return nil
		case <-timer.C:
			run(ctx)
		}
	}
}
