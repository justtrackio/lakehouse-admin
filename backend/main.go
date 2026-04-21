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
				r.POST("/expire-snapshots", httpserver.Bind(handler.ExpireSnapshots))
				r.POST("/remove-orphan-files", httpserver.Bind(handler.RemoveOrphanFiles))
				r.POST("/optimize", httpserver.Bind(handler.Optimize))
			}))

			router.Group("/api/tasks").HandleWith(httpserver.With(internal.NewHandlerTasks, func(r *httpserver.Router, handler *internal.HandlerTasks) {
				r.POST("/:id/callback-result", httpserver.Bind(handler.ProcedureResultCallback))
				r.POST("/retry-all", httpserver.BindN(handler.RetryAllTasks))
				r.POST("/retry/:id", httpserver.Bind(handler.RetryTask))
				r.POST("/by-table/:table/expire-snapshots", httpserver.Bind(handler.ExpireSnapshots))
				r.POST("/by-table/:table/remove-orphan-files", httpserver.Bind(handler.RemoveOrphanFiles))
				r.POST("/by-table/:table/optimize", httpserver.Bind(handler.Optimize))
				r.GET("", httpserver.Bind(handler.ListTasks))
				r.GET("/counts", httpserver.BindN(handler.TaskCounts))
				r.DELETE("", httpserver.BindN(handler.FlushTasks))
			}))

			router.Group("/api/settings").HandleWith(httpserver.With(internal.NewHandlerSettings, func(r *httpserver.Router, handler *internal.HandlerSettings) {
				r.GET("/task-concurrency", httpserver.BindN(handler.GetTaskConcurrency))
				r.PUT("/task-concurrency", httpserver.Bind(handler.SetTaskConcurrency))
			}))

			router.Group("/api/metadata").HandleWith(httpserver.With(internal.NewHandlerMetadata, func(r *httpserver.Router, handler *internal.HandlerMetadata) {
				r.GET("/partitions", httpserver.Bind(handler.ListPartitions))
				r.GET("/snapshots", httpserver.Bind(handler.ListSnapshots))
			}))

			router.Group("/api/refresh").HandleWith(sqlh.WithTx(internal.NewHandlerRefresh, func(r *httpserver.Router, handler *internal.HandlerRefresh) {
				r.GET("/tables", sqlh.BindTxN(handler.RefreshTables))
				r.GET("/table", sqlh.BindTx(handler.RefreshTable))
				r.GET("/table/partitions", sqlh.BindTx(handler.RefreshPartitions))
				r.GET("/table/snapshots", sqlh.BindTx(handler.RefreshSnapshots))
				r.GET("/full", sqlh.BindTxN(handler.RefreshFull))
			}))

			router.Group("/api/browse").HandleWith(httpserver.With(internal.NewHandlerBrowse, func(r *httpserver.Router, handler *internal.HandlerBrowse) {
				r.GET("/tables", httpserver.BindN(handler.ListTables))
				r.GET("/:table", httpserver.Bind(handler.TableSummary))
				r.POST("/:table/partitions", httpserver.Bind(handler.ListPartitions))
				r.POST("/:table/files", httpserver.Bind(handler.ListFiles))
			}))

			router.Group("/api/iceberg").HandleWith(httpserver.With(internal.NewHandlerIceberg, func(r *httpserver.Router, handler *internal.HandlerIceberg) {
				r.GET("/tables", httpserver.BindN(handler.ListTables))
				r.GET("/:table", httpserver.Bind(handler.DescribeTable))
				r.GET("/:table/snapshots/:snapshotId/missing-files", httpserver.Bind(handler.ListSnapshotMissingFiles))
				r.GET("/snapshots", httpserver.Bind(handler.ListSnapshots))
				r.GET("/partitions", httpserver.Bind(handler.ListPartitions))
			}))

			return nil
		})),
	).Run()
}
