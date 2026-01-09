package main

import (
	"context"
	"embed"

	"github.com/gin-contrib/cors"
	"github.com/gosoline-project/httpserver"
	"github.com/gosoline-project/sqlh"
	"github.com/justtrackio/gosoline/pkg/application"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
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
		//application.WithModuleFactory("refresh", NewModuleRefresh),
		application.WithModuleFactory("http", httpserver.NewServer("default", func(ctx context.Context, config cfg.Config, logger log.Logger, router *httpserver.Router) error {
			router.Use(cors.Default())
			router.UseFactory(httpserver.CreateEmbeddedStaticServe(publicFs, "public", "/api"))

			router.Group("/api/maintenance").HandleWith(sqlh.WithTx(NewHandlerMaintenance, func(r *httpserver.Router, handler *HandlerMaintenance) {
				r.POST("/:table/expire-snapshots", sqlh.BindTx(handler.ExpireSnapshots))
				r.POST("/:table/remove-orphan-files", sqlh.BindTx(handler.RemoveOrphanFiles))
				r.POST("/:table/optimize", sqlh.BindTx(handler.Optimize))
			}))

			router.Group("/api/metadata").HandleWith(httpserver.With(NewHandlerMetadata, func(r *httpserver.Router, handler *HandlerMetadata) {
				r.GET("/partitions", httpserver.Bind(handler.ListPartitions))
				r.GET("/snapshots", httpserver.Bind(handler.ListSnapshots))
			}))

			router.Group("/api/refresh").HandleWith(sqlh.WithTx(NewHandlerRefresh, func(r *httpserver.Router, handler *HandlerRefresh) {
				r.GET("/tables", sqlh.BindTxN(handler.RefreshTables))
				r.GET("/table", sqlh.BindTx(handler.RefreshTable))
				r.GET("/table/partitions", sqlh.BindTx(handler.RefreshPartitions))
				r.GET("/table/snapshots", sqlh.BindTx(handler.RefreshSnapshots))
				r.GET("/full", sqlh.BindTxN(handler.RefreshFull))
			}))

			router.Group("/api/browse").HandleWith(httpserver.With(NewHandlerBrowse, func(r *httpserver.Router, handler *HandlerBrowse) {
				r.GET("/tables", httpserver.BindN(handler.ListTables))
				r.GET("/:table", httpserver.Bind(handler.TableSummary))
				r.POST("/:table/partitions", httpserver.Bind(handler.ListPartitions))
			}))

			router.Group("/api/iceberg").HandleWith(httpserver.With(NewHandlerIceberg, func(r *httpserver.Router, handler *HandlerIceberg) {
				r.GET("/tables", httpserver.BindN(handler.ListTables))
				r.GET("/:table", httpserver.Bind(handler.DescribeTable))
				r.GET("/snapshots", httpserver.Bind(handler.ListSnapshots))
				r.GET("/partitions", httpserver.Bind(handler.ListPartitions))
			}))

			return nil
		})),
	).Run()
}
