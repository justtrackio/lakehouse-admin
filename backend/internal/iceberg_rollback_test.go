package internal

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/sqliteshim"
)

// newRollbackTestTable creates a table with three appended snapshots in a
// local SQLite catalog and returns the catalog and the snapshot ids, oldest first.
// Retries make iceberg-go replay a conflicting commit against the fresh head
// (Java writers often set them), which the rollback must not do.
func newRollbackTestTable(t *testing.T) (cat catalog.Catalog, ident table.Identifier, snapshots []int64) {
	return newRollbackTestTableWithRetries(t, "4")
}

func newRollbackTestTableWithRetries(t *testing.T, retries string) (cat catalog.Catalog, ident table.Identifier, snapshots []int64) {
	ctx := context.Background()
	dir := t.TempDir()

	var err error

	cat, err = catalog.Load(ctx, "test", iceberg.Properties{
		"type":                "sql",
		"uri":                 "file://" + filepath.Join(dir, "catalog.db"),
		sqlcat.DriverKey:      sqliteshim.ShimName,
		sqlcat.DialectKey:     string(sqlcat.SQLite),
		"init_catalog_tables": "true",
		"warehouse":           "file://" + dir,
	})
	require.NoError(t, err)

	ident = table.Identifier{"main", "events"}
	require.NoError(t, cat.CreateNamespace(ctx, catalog.NamespaceFromIdent(ident), nil))

	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	tbl, err := cat.CreateTable(ctx, ident, schema, catalog.WithProperties(iceberg.Properties{table.CommitNumRetriesKey: retries}))
	require.NoError(t, err)

	snapshots = make([]int64, 0, 3)
	for range 3 {
		tbl = appendRow(t, tbl)
		snapshots = append(snapshots, mainSnapshotID(tbl))
	}

	return cat, ident, snapshots
}

func appendRow(t *testing.T, tbl *table.Table) *table.Table {
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.Append(1)
	column := builder.NewArray()
	defer column.Release()

	records := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{array.NewRecordBatch(arrowSchema, []arrow.Array{column}, 1)})
	txn := tbl.NewTransaction()
	require.NoError(t, txn.AppendTable(context.Background(), records, 1, nil))

	committed, err := txn.Commit(context.Background())
	require.NoError(t, err)

	return committed
}

func loadMain(t *testing.T, cat catalog.Catalog, ident table.Identifier) int64 {
	tbl, err := cat.LoadTable(context.Background(), ident)
	require.NoError(t, err)

	return mainSnapshotID(tbl)
}

func TestRollbackToSnapshotMovesMainAndIsANoOpWhenRepeated(t *testing.T) {
	cat, ident, snapshots := newRollbackTestTable(t)
	ctx := context.Background()

	committed, err := rollbackToSnapshot(ctx, cat, ident, snapshots[0], snapshots[2])
	require.NoError(t, err)
	require.True(t, committed)
	require.Equal(t, snapshots[0], loadMain(t, cat, ident))

	// A retry, still naming the head the user saw, must not commit again.
	committed, err = rollbackToSnapshot(ctx, cat, ident, snapshots[0], snapshots[2])
	require.NoError(t, err)
	require.False(t, committed)
	require.Equal(t, snapshots[0], loadMain(t, cat, ident))
}

func TestRollbackToSnapshotRejectsAStaleHeadAndANonAncestor(t *testing.T) {
	cat, ident, snapshots := newRollbackTestTable(t)

	// The user saw snapshots[1] as the head, but main has moved on.
	_, err := rollbackToSnapshot(context.Background(), cat, ident, snapshots[0], snapshots[1])
	require.ErrorIs(t, err, ErrRollbackConflict)

	_, err = rollbackToSnapshot(context.Background(), cat, ident, 12345, snapshots[2])
	require.ErrorIs(t, err, ErrRollbackInvalidTarget)

	require.Equal(t, snapshots[2], loadMain(t, cat, ident))
}

// commitAfterLoad commits a concurrent append right after the first table load,
// so the rollback works from a head that is stale by the time it commits.
type commitAfterLoad struct {
	catalog.Catalog
	t          *testing.T
	committed  bool
	concurrent int64
}

func (c *commitAfterLoad) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, ident)
	if err != nil || c.committed {
		return tbl, err
	}

	c.committed = true
	c.concurrent = mainSnapshotID(appendRow(c.t, tbl))

	return tbl, nil
}

func TestRollbackToSnapshotFailsWhenACommitLandsBeforeItsOwn(t *testing.T) {
	cat, ident, snapshots := newRollbackTestTable(t)
	racing := &commitAfterLoad{Catalog: cat, t: t}

	committed, err := rollbackToSnapshot(context.Background(), racing, ident, snapshots[0], snapshots[2])
	require.ErrorIs(t, err, ErrRollbackConflict)
	require.False(t, committed)
	require.NotZero(t, racing.concurrent)
	require.Equal(t, racing.concurrent, loadMain(t, cat, ident))
}

// conflictOnFirstCommit stands in for Glue rejecting a commit on its VersionId
// check: before the first commit reaches the catalog, a concurrent append
// lands, and the commit fails with table.ErrCommitFailed, as Glue's
// ConcurrentModificationException does. iceberg-go then retries.
type conflictOnFirstCommit struct {
	catalog.Catalog
	t          *testing.T
	change     func(*testing.T, *table.Table) *table.Table
	conflicted bool
	concurrent int64
}

func (c *conflictOnFirstCommit) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	var err error
	var tbl *table.Table

	if tbl, err = c.Catalog.LoadTable(ctx, ident); err != nil {
		return nil, err
	}

	// Route the table's commits through this catalog.
	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}

func (c *conflictOnFirstCommit) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if !c.conflicted {
		c.conflicted = true

		tbl, err := c.Catalog.LoadTable(ctx, ident)
		require.NoError(c.t, err)
		c.concurrent = mainSnapshotID(c.change(c.t, tbl))

		return nil, "", fmt.Errorf("%w: version id mismatch", table.ErrCommitFailed)
	}

	return c.Catalog.CommitTable(ctx, ident, reqs, updates)
}

func TestRollbackToSnapshotDoesNotRetryOverACatalogConflict(t *testing.T) {
	cat, ident, snapshots := newRollbackTestTable(t)
	racing := &conflictOnFirstCommit{Catalog: cat, t: t, change: appendRow}

	committed, err := rollbackToSnapshot(context.Background(), racing, ident, snapshots[0], snapshots[2])
	require.ErrorIs(t, err, ErrRollbackConflict)
	require.False(t, committed)
	require.NotZero(t, racing.concurrent)
	require.Equal(t, racing.concurrent, loadMain(t, cat, ident))
}

func setProperty(t *testing.T, tbl *table.Table) *table.Table {
	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{"owner": "someone"}))

	committed, err := txn.Commit(context.Background())
	require.NoError(t, err)

	return committed
}

func TestRollbackToSnapshotReportsAMetadataOnlyConflict(t *testing.T) {
	// Without retries, iceberg-go returns Glue's conflict as is.
	cat, ident, snapshots := newRollbackTestTableWithRetries(t, "0")
	racing := &conflictOnFirstCommit{Catalog: cat, t: t, change: setProperty}

	committed, err := rollbackToSnapshot(context.Background(), racing, ident, snapshots[0], snapshots[2])
	require.ErrorIs(t, err, ErrRollbackConflict)
	require.False(t, committed)
	require.Equal(t, snapshots[2], loadMain(t, cat, ident))
}

func TestRollbackToSnapshotOnlyWritesConfiguredTables(t *testing.T) {
	cat, _, snapshots := newRollbackTestTable(t)
	client := &IcebergClient{catalog: cat, settings: &IcebergSettings{Databases: []string{"main"}}}

	for _, target := range [][2]string{{"other", "events"}, {"main", "other.events"}} {
		_, err := client.RollbackToSnapshot(context.Background(), target[0], target[1], snapshots[0], snapshots[2])
		require.ErrorIs(t, err, ErrRollbackInvalidTarget, "%s.%s", target[0], target[1])
	}
}

// lostResponseAfterCommit lets the first commit land, then has another writer
// append, and reports the commit as failed, as a timeout after Glue accepted
// it would.
type lostResponseAfterCommit struct {
	catalog.Catalog
	t          *testing.T
	committed  bool
	concurrent int64
}

func (c *lostResponseAfterCommit) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	var err error
	var tbl *table.Table

	if tbl, err = c.Catalog.LoadTable(ctx, ident); err != nil {
		return nil, err
	}

	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}

func (c *lostResponseAfterCommit) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if c.committed {
		return c.Catalog.CommitTable(ctx, ident, reqs, updates)
	}

	c.committed = true
	_, _, err := c.Catalog.CommitTable(ctx, ident, reqs, updates)
	require.NoError(c.t, err)

	tbl, err := c.Catalog.LoadTable(ctx, ident)
	require.NoError(c.t, err)
	c.concurrent = mainSnapshotID(appendRow(c.t, tbl))

	return nil, "", errors.New("context deadline exceeded")
}

func TestRollbackToSnapshotReportsARollbackThatLandedBeforeTheTableMovedOn(t *testing.T) {
	cat, ident, snapshots := newRollbackTestTable(t)
	lost := &lostResponseAfterCommit{Catalog: cat, t: t}

	committed, err := rollbackToSnapshot(context.Background(), lost, ident, snapshots[0], snapshots[2])
	require.NoError(t, err)
	require.True(t, committed)
	require.Equal(t, lost.concurrent, loadMain(t, cat, ident))
}

// unreachableAfterCommit fails the commit and every load after it, as a lost
// connection to the catalog would.
type unreachableAfterCommit struct {
	catalog.Catalog
	commitFailed bool
}

func (c *unreachableAfterCommit) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	var err error
	var tbl *table.Table

	if c.commitFailed {
		return nil, errors.New("connection reset")
	}

	if tbl, err = c.Catalog.LoadTable(ctx, ident); err != nil {
		return nil, err
	}

	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}

func (c *unreachableAfterCommit) CommitTable(context.Context, table.Identifier, []table.Requirement, []table.Update) (table.Metadata, string, error) {
	c.commitFailed = true

	return nil, "", errors.New("context deadline exceeded")
}

func TestRollbackToSnapshotReportsAnUnknownOutcomeWhenTheReloadFails(t *testing.T) {
	cat, ident, snapshots := newRollbackTestTable(t)

	_, err := rollbackToSnapshot(context.Background(), &unreachableAfterCommit{Catalog: cat}, ident, snapshots[0], snapshots[2])
	require.ErrorIs(t, err, ErrRollbackOutcomeUnknown)
}

// timeoutBeforeCommit fails the commit without applying it, as a timeout
// whose request never reached the catalog would.
type timeoutBeforeCommit struct{ catalog.Catalog }

func (c timeoutBeforeCommit) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	var err error
	var tbl *table.Table

	if tbl, err = c.Catalog.LoadTable(ctx, ident); err != nil {
		return nil, err
	}

	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}

func (timeoutBeforeCommit) CommitTable(context.Context, table.Identifier, []table.Requirement, []table.Update) (table.Metadata, string, error) {
	return nil, "", errors.New("context deadline exceeded")
}

func TestRollbackToSnapshotReportsAnUnknownOutcomeWhenAnAmbiguousErrorLeftMainInPlace(t *testing.T) {
	cat, ident, snapshots := newRollbackTestTable(t)

	_, err := rollbackToSnapshot(context.Background(), timeoutBeforeCommit{Catalog: cat}, ident, snapshots[0], snapshots[2])
	require.ErrorIs(t, err, ErrRollbackOutcomeUnknown)
	require.NotErrorIs(t, err, ErrRollbackConflict)
	require.Equal(t, snapshots[2], loadMain(t, cat, ident))
}
