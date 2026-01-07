package main

import (
	"context"
	_ "embed"

	"github.com/gin-contrib/cors"
	"github.com/gosoline-project/httpserver"
	"github.com/justtrackio/gosoline/pkg/application"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

//go:embed config.dist.yml
var configDist []byte

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

			router.Group("/api/maintenance").HandleWith(httpserver.With(NewHandlerMaintenance, func(r *httpserver.Router, handler *HandlerMaintenance) {
				r.POST("/:table/expire-snapshots", httpserver.Bind(handler.ExpireSnapshots))
			}))

			router.Group("/api/metadata").HandleWith(httpserver.With(NewHandlerMetadata, func(r *httpserver.Router, handler *HandlerMetadata) {
				r.GET("/partitions", httpserver.Bind(handler.ListPartitions))
				r.GET("/snapshots", httpserver.Bind(handler.ListSnapshots))
			}))

			router.Group("/api/refresh").HandleWith(httpserver.With(NewHandlerRefresh, func(r *httpserver.Router, handler *HandlerRefresh) {
				r.GET("/tables", httpserver.BindN(handler.RefreshTables))
				r.GET("/table", httpserver.Bind(handler.RefreshTable))
				r.GET("/table/partitions", httpserver.Bind(handler.RefreshPartitions))
				r.GET("/table/snapshots", httpserver.Bind(handler.RefreshSnapshots))
				r.GET("/full", httpserver.BindN(handler.RefreshFull))
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
