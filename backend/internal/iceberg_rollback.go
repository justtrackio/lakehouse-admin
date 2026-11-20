package internal

import (
	"context"
	"errors"
	"fmt"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
)

// ErrRollbackConflict reports that the table's main branch is no longer at the
// snapshot the caller expected, so rolling back would discard newer commits.
var ErrRollbackConflict = errors.New("the table changed since it was loaded")

// ErrRollbackInvalidTarget reports a target snapshot that is not an ancestor of
// the table's main branch.
var ErrRollbackInvalidTarget = errors.New("invalid rollback target")

// RollbackToSnapshot moves the main branch of the table back to targetID, if
// main is still at expectedID. It returns whether it committed.
func (c *IcebergClient) RollbackToSnapshot(ctx context.Context, database string, logicalName string, targetID int64, expectedID int64) (bool, error) {
	ctx = utils.WithAwsConfig(ctx, &c.awsCfg)

	return rollbackToSnapshot(ctx, c.catalog, c.resolveTableIdentifier(database, logicalName), targetID, expectedID)
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
		return resolveFailedRollbackCommit(ctx, cat, ident, targetID, expectedID, err)
	}

	return true, nil
}

// resolveFailedRollbackCommit reloads the table to tell a conflict from a
// commit that landed despite an error, such as a timeout after the catalog
// accepted it.
func resolveFailedRollbackCommit(ctx context.Context, cat catalog.Catalog, ident table.Identifier, targetID int64, expectedID int64, commitErr error) (bool, error) {
	var err error
	var tbl *table.Table

	if tbl, err = cat.LoadTable(ctx, ident); err != nil {
		return false, fmt.Errorf("could not commit the rollback (%w), and could not reload the table to check: %w", commitErr, err)
	}

	switch mainSnapshotID(tbl) {
	case targetID:
		return true, nil
	case expectedID:
		return false, fmt.Errorf("could not commit the rollback: %w", commitErr)
	default:
		return false, fmt.Errorf("%w: main moved to snapshot %d during the rollback: %w", ErrRollbackConflict, mainSnapshotID(tbl), commitErr)
	}
}

func mainSnapshotID(tbl *table.Table) int64 {
	if snapshot := tbl.Metadata().SnapshotByName(table.MainBranch); snapshot != nil {
		return snapshot.SnapshotID
	}

	return 0
}
