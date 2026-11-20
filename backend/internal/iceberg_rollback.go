package internal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
)

// ErrRollbackConflict reports that the table's main branch is no longer at the
// snapshot the caller expected, so rolling back would discard newer commits.
var ErrRollbackConflict = errors.New("the table changed since it was loaded")

// ErrRollbackOutcomeUnknown reports a failed commit after which the table
// doesn't show whether the rollback landed.
var ErrRollbackOutcomeUnknown = errors.New("could not tell whether the rollback was committed; check the table's snapshot history before retrying")

// ErrRollbackInvalidTarget reports a rollback of a table outside the
// configured databases, or to a snapshot that is not an ancestor of main.
var ErrRollbackInvalidTarget = errors.New("invalid rollback target")

// RollbackToSnapshot moves the main branch of database.table back to targetID,
// if main is still at expectedID. It returns whether it committed.
func (c *IcebergClient) RollbackToSnapshot(ctx context.Context, database string, tableName string, targetID int64, expectedID int64) (bool, error) {
	// A dotted name would pick its own database (resolveTableIdentifier), so the
	// write could hit a different table than the refresh after it.
	if strings.Contains(tableName, ".") || !slices.Contains(c.settings.Databases, database) {
		return false, fmt.Errorf("%w: table %s.%s is not in a configured database", ErrRollbackInvalidTarget, database, tableName)
	}

	ctx = utils.WithAwsConfig(ctx, &c.awsCfg)

	return rollbackToSnapshot(ctx, c.catalog, table.Identifier{database, tableName}, targetID, expectedID)
}

// rollbackToSnapshot commits main = targetID in one transaction that requires
// main to still be at expectedID when the catalog applies it. A main already at
// targetID needs no commit, which makes a retry after a successful rollback
// safe.
func rollbackToSnapshot(ctx context.Context, cat catalog.Catalog, ident table.Identifier, targetID int64, expectedID int64) (bool, error) {
	var err error
	var tbl *table.Table

	if tbl, err = cat.LoadTable(ctx, ident); err != nil {
		return false, fmt.Errorf("could not load table %s: %w", ident, err)
	}

	switch mainSnapshotID(tbl) {
	case targetID:
		return false, nil
	case expectedID:
	default:
		return false, fmt.Errorf("%w: main is at snapshot %d, expected %d", ErrRollbackConflict, mainSnapshotID(tbl), expectedID)
	}

	txn := tbl.NewTransaction()

	// Pin main to the loaded head. Unpinned, a conflicting commit makes
	// iceberg-go retry against the fresh head, which would roll back over it.
	if err = txn.AssertRefSnapshotID(table.MainBranch); err != nil {
		return false, fmt.Errorf("could not pin the main branch: %w", err)
	}

	if err = txn.RollbackToSnapshot(targetID); err != nil {
		if errors.Is(err, iceberg.ErrInvalidArgument) {
			return false, fmt.Errorf("%w: %w", ErrRollbackInvalidTarget, err)
		}

		return false, fmt.Errorf("could not stage the rollback: %w", err)
	}

	if _, err = txn.Commit(ctx); err != nil {
		return resolveFailedRollbackCommit(ctx, cat, tbl.Metadata(), ident, targetID, expectedID, err)
	}

	return true, nil
}

// resolveFailedRollbackCommit reloads the table to tell a conflict from a
// commit that landed despite an error, such as a timeout after the catalog
// accepted it.
func resolveFailedRollbackCommit(ctx context.Context, cat catalog.Catalog, base table.Metadata, ident table.Identifier, targetID int64, expectedID int64, commitErr error) (bool, error) {
	var err error
	var tbl *table.Table

	if tbl, err = cat.LoadTable(ctx, ident); err != nil {
		return false, fmt.Errorf("%w: the commit failed (%w), and reloading the table to check failed: %w", ErrRollbackOutcomeUnknown, commitErr, err)
	}

	switch mainSnapshotID(tbl) {
	case targetID:
		return true, nil
	case expectedID:
		// Another writer changed only table metadata (Glue's VersionId check);
		// nothing was rolled back, and the caller may retry.
		if errors.Is(commitErr, table.ErrCommitFailed) {
			return false, fmt.Errorf("%w: the table metadata changed during the rollback: %w", ErrRollbackConflict, commitErr)
		}

		return false, fmt.Errorf("could not commit the rollback: %w", commitErr)
	}

	// main moved on. The snapshot log since the loaded metadata tells whether
	// our commit made the target current before another writer moved it.
	landed, known := targetBecameCurrent(base, tbl.Metadata(), targetID)
	switch {
	case !known:
		return false, fmt.Errorf("%w: main is at snapshot %d: %w", ErrRollbackOutcomeUnknown, mainSnapshotID(tbl), commitErr)
	case landed:
		return true, nil
	default:
		return false, fmt.Errorf("%w: main moved to snapshot %d during the rollback: %w", ErrRollbackConflict, mainSnapshotID(tbl), commitErr)
	}
}

// targetBecameCurrent reports whether fresh's snapshot log records targetID as
// current after the last entry base knew. known is false if base's last entry
// is no longer in the log.
func targetBecameCurrent(base table.Metadata, fresh table.Metadata, targetID int64) (landed bool, known bool) {
	baseLog := slices.Collect(base.SnapshotLogs())
	freshLog := slices.Collect(fresh.SnapshotLogs())

	start := 0
	if len(baseLog) > 0 {
		if start = slices.Index(freshLog, baseLog[len(baseLog)-1]) + 1; start == 0 {
			return false, false
		}
	}

	for _, entry := range freshLog[start:] {
		if entry.SnapshotID == targetID {
			return true, true
		}
	}

	return false, true
}

func mainSnapshotID(tbl *table.Table) int64 {
	if snapshot := tbl.Metadata().SnapshotByName(table.MainBranch); snapshot != nil {
		return snapshot.SnapshotID
	}

	return 0
}
