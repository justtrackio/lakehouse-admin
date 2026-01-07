package main

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/spark-connect-go/spark/sql"
	"github.com/apache/spark-connect-go/spark/sql/types"
	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/db"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/gosoline/pkg/mapx"
	"github.com/justtrackio/gosoline/pkg/refl"
	"github.com/spf13/cast"
)

type SparkSettings struct {
	Endpoint string `cfg:"endpoint"`
}

func ProvideSparkClient(ctx context.Context, config cfg.Config, logger log.Logger) (*SparkClient, error) {
	return appctx.Provide(ctx, struct{}{}, func() (*SparkClient, error) {
		var err error
		var session sql.SparkSession

		settings := &SparkSettings{}
		if err = config.UnmarshalKey("spark", settings); err != nil {
			return nil, fmt.Errorf("could not unmarshal spark settings: %w", err)
		}

		if session, err = sql.NewSessionBuilder().Remote(settings.Endpoint).Build(ctx); err != nil {
			return nil, fmt.Errorf("failed to create spark session: %w", err)
		}

		return &SparkClient{
			session: session,
		}, nil
	})
}

type SparkClient struct {
	session sql.SparkSession
}

func (c *SparkClient) DescribeTable(ctx context.Context, table string) (*TableDescription, error) {
	var err error
	var msi []map[string]any

	if msi, err = c.QueryRows(ctx, fmt.Sprintf("DESCRIBE TABLE main.%s", table)); err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	rows := make([]map[string]string, len(msi))
	for i, row := range msi {
		if rows[i], err = cast.ToStringMapStringE(row); err != nil {
			return nil, fmt.Errorf("failed to cast describe table result: %w", err)
		}
	}

	columns := make(TableColumns, 0)
	partitions := make([]TablePartition, 0)

	i := 0
	for ; i < len(rows); i++ {
		if rows[i]["col_name"] == "# Partitioning" {
			break
		}

		if rows[i]["col_name"] == "" {
			continue
		}

		columns = append(columns, TableColumn{
			Name: fmt.Sprint(rows[i]["col_name"]),
			Type: fmt.Sprint(rows[i]["data_type"]),
		})
	}

	var re = regexp.MustCompile(`(?m)(\w+)\(([\w\d\.]+)\)`)

	for i++; i < len(rows); i++ {
		matches := re.FindAllStringSubmatch(rows[i]["data_type"], -1)

		if len(matches) == 0 {
			partitions = append(partitions, TablePartition{
				Name: rows[i]["data_type"],
			})

			continue
		}

		switch matches[0][1] {
		case "days":
			partitions = append(partitions, []TablePartition{
				{"year", true, TablePartitionHidden{matches[0][2], "day"}},
				{"month", true, TablePartitionHidden{matches[0][2], "day"}},
				{"day", true, TablePartitionHidden{matches[0][2], "day"}},
			}...)
		case "months":
			partitions = append(partitions, []TablePartition{
				{"year", true, TablePartitionHidden{matches[0][2], "month"}},
				{"month", true, TablePartitionHidden{matches[0][2], "month"}},
			}...)
		case "years":
			partitions = append(partitions, []TablePartition{
				{"year", true, TablePartitionHidden{matches[0][2], "year"}},
			}...)
		}
	}

	desc := &TableDescription{
		Name:       table,
		Columns:    db.NewJSON(columns, db.NonNullable{}),
		Partitions: db.NewJSON(partitions, db.NonNullable{}),
		UpdatedAt:  time.Now(),
	}

	return desc, nil
}

func (c *SparkClient) ListPartitions(ctx context.Context, table string) ([]sPartition, error) {
	var err error

	query := fmt.Sprintf("SELECT * FROM main.%s.partitions", table)
	result := make([]sPartition, 0)

	if err = c.Query(ctx, query, &result); err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return result, err
}

func (c *SparkClient) ListSnapshots(ctx context.Context, table string) ([]sSnapshot, error) {
	var err error

	query := fmt.Sprintf("SELECT * FROM main.%s.snapshots", table)
	result := make([]sSnapshot, 0)

	if err = c.Query(ctx, query, &result); err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return result, err
}

func (c *SparkClient) ListTables(ctx context.Context) ([]string, error) {
	var err error
	var rows []map[string]any

	if rows, err = c.QueryRows(ctx, "SHOW TABLES FROM main"); err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	tables := make([]string, 0, len(rows))
	for _, row := range rows {
		tables = append(tables, fmt.Sprint(row["tableName"]))
	}

	return tables, nil
}

func (c *SparkClient) Call(ctx context.Context, query string, result any) error {
	var err error
	var rows []map[string]any
	var slice *refl.Slice
	var ms *mapx.Struct

	if rows, err = c.QueryRows(ctx, query); err != nil {
		return fmt.Errorf("failed to collect rows: %w", err)
	}

	if slice, err = refl.SliceOf(result); err != nil {
		return fmt.Errorf("failed to get slice of result: %w", err)
	}

	for _, row := range rows {
		elem := slice.NewElement()

		if ms, err = c.getStructWriter(elem); err != nil {
			return fmt.Errorf("failed to create map struct: %w", err)
		}

		mapx := mapx.NewMapX(row)
		if err = ms.Write(mapx); err != nil {
			return fmt.Errorf("failed to write map struct: %w", err)
		}

		slice.Append(elem)
	}

	return nil
}

func (c *SparkClient) Query(ctx context.Context, query string, result any) error {
	var err error
	var rows []map[string]any
	var slice *refl.Slice
	var ms *mapx.Struct

	if rows, err = c.QueryRowsPaged(ctx, query); err != nil {
		return fmt.Errorf("failed to collect rows: %w", err)
	}

	if slice, err = refl.SliceOf(result); err != nil {
		return fmt.Errorf("failed to get slice of result: %w", err)
	}

	for _, row := range rows {
		elem := slice.NewElement()

		if ms, err = c.getStructWriter(elem); err != nil {
			return fmt.Errorf("failed to create map struct: %w", err)
		}

		mapx := mapx.NewMapX(row)
		if err = ms.Write(mapx); err != nil {
			return fmt.Errorf("failed to write map struct: %w", err)
		}

		slice.Append(elem)
	}

	return nil
}

func (c *SparkClient) QueryRows(ctx context.Context, query string) ([]map[string]any, error) {
	var err error
	var df sql.DataFrame
	var columns []string
	var rows []types.Row

	if df, err = c.session.Sql(ctx, query); err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	if columns, err = df.Columns(ctx); err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	if rows, err = df.Collect(ctx); err != nil {
		return nil, fmt.Errorf("failed to collect rows: %w", err)
	}

	result := make([]map[string]any, len(rows))
	for i, row := range rows {
		result[i] = make(map[string]any)

		for _, col := range columns {
			val := row.Value(col)

			if maa, ok := val.(map[any]any); ok {
				if val, err = cast.ToStringMapE(maa); err != nil {
					return nil, fmt.Errorf("failed to cast column %s to map[string]any: %w", col, err)
				}
			}

			result[i][col] = val
		}
	}

	return result, nil
}

func (c *SparkClient) QueryRowsPaged(ctx context.Context, query string) ([]map[string]any, error) {
	var err error
	var df sql.DataFrame
	var columns []string
	var rows []types.Row

	offset := 0
	limit := 1000
	result := make([]map[string]any, 0)

	for {
		limitQuery := fmt.Sprintf("%s LIMIT %d OFFSET %d", query, limit, offset)

		if df, err = c.session.Sql(ctx, limitQuery); err != nil {
			return nil, fmt.Errorf("failed to execute query after offset %d: %w", offset, err)
		}

		if columns, err = df.Columns(ctx); err != nil {
			return nil, fmt.Errorf("failed to get columns after offset %d: %w", offset, err)
		}

		if rows, err = df.Collect(ctx); err != nil {
			return nil, fmt.Errorf("failed to collect rows after offset %d: %w", offset, err)
		}

		for _, r := range rows {
			row := make(map[string]any)

			for _, col := range columns {
				val := r.Value(col)

				if maa, ok := val.(map[any]any); ok {
					if val, err = cast.ToStringMapE(maa); err != nil {
						return nil, fmt.Errorf("failed to cast column %s to map[string]any: %w", col, err)
					}
				}

				row[col] = val
			}

			result = append(result, row)
		}

		if len(rows) < limit {
			break
		}

		offset += limit
	}

	return result, nil
}

func (c *SparkClient) getStructWriter(val any) (*mapx.Struct, error) {
	return mapx.NewStruct(val, &mapx.StructSettings{
		FieldTag:   "json",
		DefaultTag: "default",
		Casters: []mapx.MapStructCaster{
			mapx.MapStructDurationCaster,
			ArrowTimeToTime,
			ArrowDateToTime,
			mapx.MapStructTimeCaster,
		},
	})
}

func ArrowTimeToTime(targetType reflect.Type, value any) (any, error) {
	if targetType != reflect.TypeOf(time.Time{}) {
		return nil, nil
	}

	if val, ok := value.(arrow.Timestamp); ok {
		return val.ToTime(arrow.Microsecond), nil
	}

	return nil, nil
}

func ArrowDateToTime(targetType reflect.Type, value any) (any, error) {
	if targetType != reflect.TypeOf(time.Time{}) {
		return nil, nil
	}

	if val, ok := value.(arrow.Date32); ok {
		return val.ToTime(), nil
	}

	return nil, nil
}
