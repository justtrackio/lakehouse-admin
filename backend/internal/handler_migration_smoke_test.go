package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gosoline-project/httpserver"
	"github.com/gosoline-project/sqlc"
	sqlcmocks "github.com/gosoline-project/sqlc/mocks"
	"github.com/gosoline-project/sqlh"
	gosolog "github.com/justtrackio/gosoline/pkg/log"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type migrationSmokeSQLClient struct {
	*sqlcmocks.Client
	tx           sqlc.Tx
	beginTxCalls int
}

func (c *migrationSmokeSQLClient) BeginTx(ctx context.Context, _ ...*sql.TxOptions) (sqlc.Tx, error) {
	c.beginTxCalls++
	if tx, ok := c.tx.(*migrationSmokeSQLTx); ok {
		tx.ctx = ctx
	}

	return c.tx, nil
}

func (c *migrationSmokeSQLClient) WithTx(ctx context.Context, operation func(sqlc.Tx) error, options ...*sql.TxOptions) error {
	var tx sqlc.Tx
	var err error
	if tx, err = c.BeginTx(ctx, options...); err != nil {
		return err
	}
	if err = operation(tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}

		return err
	}

	return tx.Commit()
}

type migrationSmokeSQLTx struct {
	ctx context.Context
	*sqlcmocks.Tx
	queryBuilder      *sqlc.QueryBuilder
	queryBuilderCalls *int
}

func (tx *migrationSmokeSQLTx) Deadline() (time.Time, bool) {
	return tx.ctx.Deadline()
}

func (tx *migrationSmokeSQLTx) Done() <-chan struct{} {
	return tx.ctx.Done()
}

func (tx *migrationSmokeSQLTx) Err() error {
	return tx.ctx.Err()
}

func (tx *migrationSmokeSQLTx) Value(key any) any {
	return tx.ctx.Value(key)
}

func (tx *migrationSmokeSQLTx) Q() *sqlc.QueryBuilder {
	*tx.queryBuilderCalls++

	return tx.queryBuilder
}

func (tx *migrationSmokeSQLTx) SQLTx() *sql.Tx {
	return nil
}

func (tx *migrationSmokeSQLTx) WithContext(ctx context.Context) sqlc.Tx {
	return &migrationSmokeSQLTx{
		ctx:               ctx,
		Tx:                tx.Tx,
		queryBuilder:      tx.queryBuilder,
		queryBuilderCalls: tx.queryBuilderCalls,
	}
}

type metadataSmokeHandler func(context.Context, *TableSelectInput) (httpserver.Response, error)

func TestMetadataPartitionsHTTPRunsScopedQuery(t *testing.T) {
	assertMetadataHTTPQuery(t, "partitions", func(handler *HandlerMetadata) metadataSmokeHandler {
		return handler.ListPartitions
	})
}

func TestMetadataSnapshotsHTTPRunsScopedQuery(t *testing.T) {
	assertMetadataHTTPQuery(t, "snapshots", func(handler *HandlerMetadata) metadataSmokeHandler {
		return handler.ListSnapshots
	})
}

func assertMetadataHTTPQuery(t *testing.T, resource string, newHandler func(*HandlerMetadata) metadataSmokeHandler) {
	t.Helper()

	clientMock := sqlcmocks.NewClient(t)
	querier := sqlcmocks.NewQuerier(t)
	clientMock.EXPECT().Q().Return(sqlc.NewQueryBuilder(querier, nil)).Once()

	var generatedQuery string
	var generatedArgs []any
	querier.EXPECT().Select(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ any, query string, args ...any) {
			generatedQuery = query
			generatedArgs = args
		}).Return(nil).Once()

	handler := newHandler(&HandlerMetadata{sqlClient: &migrationSmokeSQLClient{Client: clientMock}})
	response := httpserver.HttpTest(
		http.MethodGet,
		"/api/metadata/:database/:table/"+resource,
		"/api/metadata/main/revenueEvent/"+resource,
		"",
		httpserver.Bind(func(ctx context.Context, input *TableSelectInput) (httpserver.Response, error) {
			return handler(ctx, input)
		}),
	)

	require.Equal(t, http.StatusOK, response.Code)
	require.Contains(t, response.Header().Get("Content-Type"), "application/json")
	require.JSONEq(t, "[]", response.Body.String())
	query := strings.ToLower(generatedQuery)
	require.Contains(t, query, "select")
	require.Contains(t, query, resource)
	require.Contains(t, query, "database")
	require.Contains(t, query, "table")
	require.ElementsMatch(t, []any{"main", "revenueEvent"}, generatedArgs)
}

type migrationSmokeIceberg struct {
	partitions []IcebergPartition
	database   string
	table      string
}

func (s *migrationSmokeIceberg) ListDatabases(context.Context) ([]CatalogDatabase, error) {
	return nil, nil
}

func (s *migrationSmokeIceberg) ListTables(context.Context, string) ([]CatalogTable, error) {
	return nil, nil
}

func (s *migrationSmokeIceberg) DescribeTable(context.Context, string, string) (*TableDescription, error) {
	return nil, nil
}

func (s *migrationSmokeIceberg) ListPartitions(_ context.Context, database, table string) ([]IcebergPartition, error) {
	s.database = database
	s.table = table

	return s.partitions, nil
}

func (s *migrationSmokeIceberg) ListSnapshots(context.Context, string, string) ([]IcebergSnapshot, error) {
	return nil, nil
}

func TestRefreshPartitionsHTTPCommitsGeneratedQueries(t *testing.T) {
	txMock := sqlcmocks.NewTx(t)
	queryBuilderCalls := 0
	tx := &migrationSmokeSQLTx{ctx: context.Background(), Tx: txMock, queryBuilderCalls: &queryBuilderCalls}
	tx.queryBuilder = sqlc.NewQueryBuilder(txMock, nil)
	clientMock := sqlcmocks.NewClient(t)
	client := &migrationSmokeSQLClient{Client: clientMock, tx: tx}

	var deleteQuery string
	var deleteArgs []any
	var insertQuery string
	var insertRecord any
	txMock.EXPECT().Exec(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, query string, args ...any) {
			deleteQuery = query
			deleteArgs = args
		}).Return(nil, nil).Once()
	txMock.EXPECT().NamedExec(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, query string, arg any) {
			insertQuery = query
			insertRecord = arg
		}).Return(nil, nil).Once()
	txMock.EXPECT().Commit().Return(nil).Once()

	runner, err := sqlh.NewTxRunnerWithClient(client)
	require.NoError(t, err)
	lastUpdatedAt := time.Date(2026, time.October, 13, 12, 0, 0, 0, time.UTC)
	iceberg := &migrationSmokeIceberg{partitions: []IcebergPartition{{
		Partition:         PartitionValues{"region": "eu-central-1"},
		SpecID:            7,
		RecordCount:       123,
		FileCount:         4,
		DataFileSizeBytes: 2048,
		NeedsOptimize:     true,
		LastUpdatedAt:     lastUpdatedAt,
		LastSnapshotID:    99,
	}}}
	handler := &HandlerRefresh{
		service:  &ServiceRefresh{iceberg: iceberg, logger: gosolog.NewLogger()},
		txRunner: runner,
	}

	response := httpserver.HttpTest(
		http.MethodGet,
		"/api/refresh/:database/:table/partitions",
		"/api/refresh/main/revenueEvent/partitions",
		"",
		httpserver.Bind(handler.RefreshPartitions),
	)

	require.Equal(t, http.StatusOK, response.Code)
	require.Contains(t, response.Header().Get("Content-Type"), "application/json")
	var result []map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.Len(t, result, 1)
	require.Equal(t, "main", result[0]["database"])
	require.Equal(t, "revenueEvent", result[0]["table"])
	require.Equal(t, map[string]any{"region": "eu-central-1"}, result[0]["partition"])
	require.Equal(t, float64(7), result[0]["spec_id"])
	require.Equal(t, float64(123), result[0]["record_count"])
	require.Equal(t, float64(4), result[0]["file_count"])
	require.Equal(t, float64(2048), result[0]["total_data_file_size_in_bytes"])
	require.Equal(t, "99", result[0]["last_updated_snapshot_id"])
	require.Equal(t, true, result[0]["needs_optimize"])
	require.Equal(t, "main", iceberg.database)
	require.Equal(t, "revenueEvent", iceberg.table)
	require.Equal(t, 1, client.beginTxCalls)
	require.Equal(t, 2, queryBuilderCalls)

	deleteQuery = strings.ToLower(deleteQuery)
	insertQuery = strings.ToLower(insertQuery)
	require.Contains(t, deleteQuery, "delete")
	require.Contains(t, deleteQuery, "partitions")
	require.ElementsMatch(t, []any{"main", "revenueEvent"}, deleteArgs)
	require.Contains(t, insertQuery, "insert")
	require.Contains(t, insertQuery, "partitions")
	inserted, ok := insertRecord.(Partition)
	require.True(t, ok)
	require.Equal(t, "main", inserted.Database)
	require.Equal(t, "revenueEvent", inserted.Table)
	require.Equal(t, PartitionValues{"region": "eu-central-1"}, inserted.Partition.Get())
	require.Equal(t, 7, inserted.SpecId)
	require.Equal(t, int64(123), inserted.RecordCount)
	require.Equal(t, int64(4), inserted.FileCount)
	require.Equal(t, int64(2048), inserted.TotalDataFileSizeInBytes)
	require.Equal(t, lastUpdatedAt, inserted.LastUpdatedAt)
	require.Equal(t, int64(99), inserted.LastUpdatedSnapshotId)
	require.True(t, inserted.NeedsOptimize)
}
