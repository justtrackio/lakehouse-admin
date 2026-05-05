package main

import (
	"context"
	"embed"

	"github.com/gin-contrib/cors"
	"github.com/gosoline-project/httpserver"
	"github.com/gosoline-project/sqlh"
	"github.com/justtrackio/gosoline/pkg/application"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/kernel"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/lakehouse-admin/internal"
)

//go:embed config.dist.yml
var configDist []byte

//go:embed public
var publicFs embed.FS

func main() {
	application.New(
		application.WithConfigDebug,
		application.WithConfigBytes(configDist, "yml"),
		application.WithConfigEnvKeyReplacer(cfg.DefaultEnvKeyReplacer),
		application.WithConfigFileFlag,
		application.WithConfigSanitizers(cfg.TimeSanitizer),
		application.WithLoggerHandlersFromConfig,
		application.WithUTCClock(true),
		application.WithModuleFactory("tasks", func(ctx context.Context, config cfg.Config, logger log.Logger) (kernel.Module, error) {
			return internal.ProvideModuleTasks(ctx, config, logger)
		}),
		application.WithModuleFactory("maintenance_schedule", internal.NewModuleMaintenanceSchedule),
		application.WithModuleFactory("refresh", internal.NewModuleRefresh),
		application.WithModuleFactory("http", httpserver.NewServer("default", func(ctx context.Context, config cfg.Config, logger log.Logger, router *httpserver.Router) error {
			router.Use(cors.Default())
			router.UseFactory(httpserver.CreateEmbeddedStaticServe(publicFs, "public", "/api"))

			router.Group("/api/maintenance").HandleWith(httpserver.With(internal.NewHandlerMaintenance, func(r *httpserver.Router, handler *internal.HandlerMaintenance) {
				r.POST("/:database/expire-snapshots", httpserver.Bind(handler.ExpireSnapshots))
				r.POST("/:database/remove-orphan-files", httpserver.Bind(handler.RemoveOrphanFiles))
				r.POST("/:database/optimize", httpserver.Bind(handler.Optimize))
			}))

			router.Group("/api/tasks").HandleWith(httpserver.With(internal.NewHandlerTasks, func(r *httpserver.Router, handler *internal.HandlerTasks) {
				r.GET("", httpserver.Bind(handler.ListAllTasks))
				r.GET("/counts", httpserver.BindN(handler.AllTaskCounts))
				r.DELETE("", httpserver.BindN(handler.FlushAllTasks))
				r.POST("/retry-all", httpserver.BindN(handler.RetryAllTasksGlobal))
				r.POST("/callback/:id/result", httpserver.Bind(handler.ProcedureResultCallback))
				r.POST("/:database/retry-all", httpserver.Bind(handler.RetryAllTasks))
				r.POST("/retry/:id", httpserver.Bind(handler.RetryTask))
				r.POST("/:database/:table/expire-snapshots", httpserver.Bind(handler.ExpireSnapshots))
				r.POST("/:database/:table/remove-orphan-files", httpserver.Bind(handler.RemoveOrphanFiles))
				r.POST("/:database/:table/optimize", httpserver.Bind(handler.Optimize))
				r.GET("/:database", httpserver.Bind(handler.ListTasks))
				r.GET("/:database/counts", httpserver.Bind(handler.TaskCounts))
				r.DELETE("/:database", httpserver.Bind(handler.FlushTasks))
			}))

			router.Group("/api/settings").HandleWith(httpserver.With(internal.NewHandlerSettings, func(r *httpserver.Router, handler *internal.HandlerSettings) {
				r.GET("/task-concurrency", httpserver.BindN(handler.GetTaskConcurrency))
				r.PUT("/task-concurrency", httpserver.Bind(handler.SetTaskConcurrency))
			}))

			router.Group("/api/metadata").HandleWith(httpserver.With(internal.NewHandlerMetadata, func(r *httpserver.Router, handler *internal.HandlerMetadata) {
				r.GET("/:database/:table/partitions", httpserver.Bind(handler.ListPartitions))
				r.GET("/:database/:table/snapshots", httpserver.Bind(handler.ListSnapshots))
			}))

			router.Group("/api/refresh").HandleWith(sqlh.WithTx(internal.NewHandlerRefresh, func(r *httpserver.Router, handler *internal.HandlerRefresh) {
				r.GET("/tables", sqlh.BindTxN(handler.RefreshTables))
				r.GET("/:database/:table", sqlh.BindTx(handler.RefreshTable))
				r.GET("/:database/:table/partitions", sqlh.BindTx(handler.RefreshPartitions))
				r.GET("/:database/:table/snapshots", sqlh.BindTx(handler.RefreshSnapshots))
				r.GET("/full", sqlh.BindTxN(handler.RefreshFull))
			}))

			router.Group("/api/browse").HandleWith(httpserver.With(internal.NewHandlerBrowse, func(r *httpserver.Router, handler *internal.HandlerBrowse) {
				r.GET("/:database/tables", httpserver.Bind(handler.ListTables))
				r.GET("/:database/:table", httpserver.Bind(handler.TableSummary))
				r.POST("/:database/:table/partitions", httpserver.Bind(handler.ListPartitions))
				r.POST("/:database/:table/files", httpserver.Bind(handler.ListFiles))
			}))

			router.Group("/api/iceberg").HandleWith(httpserver.With(internal.NewHandlerIceberg, func(r *httpserver.Router, handler *internal.HandlerIceberg) {
				r.GET("/databases", httpserver.BindN(handler.ListDatabases))
				r.GET("/:database/tables", httpserver.Bind(handler.ListTables))
				r.GET("/:database/:table", httpserver.Bind(handler.DescribeTable))
				r.POST("/:database/:table/snapshots/:snapshotId/rollback", httpserver.Bind(handler.RollbackToSnapshot))
				r.GET("/:database/:table/snapshots/:snapshotId/missing-files", httpserver.Bind(handler.ListSnapshotMissingFiles))
				r.GET("/:database/:table/snapshots", httpserver.Bind(handler.ListSnapshots))
				r.GET("/:database/:table/partitions", httpserver.Bind(handler.ListPartitions))
			}))

			return nil
		})),
	).Run()
}
