package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOptimizeRangeWithinDelayClampsUpperBound(t *testing.T) {
	now := time.Date(2026, time.March, 31, 12, 0, 0, 0, time.UTC)

	rangeChunk, ok := optimizeRangeWithinDelay(
		time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, time.March, 31, 0, 0, 0, 0, time.UTC),
		now,
		48*time.Hour,
	)

	require.True(t, ok)
	require.Equal(t, time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC), rangeChunk.from)
	require.Equal(t, time.Date(2026, time.March, 29, 0, 0, 0, 0, time.UTC), rangeChunk.to)
}

func TestOptimizeRangeWithinDelayReturnsEmptyWhenWindowTooRecent(t *testing.T) {
	now := time.Date(2026, time.March, 31, 12, 0, 0, 0, time.UTC)

	_, ok := optimizeRangeWithinDelay(
		time.Date(2026, time.March, 30, 0, 0, 0, 0, time.UTC),
		time.Date(2026, time.March, 31, 0, 0, 0, 0, time.UTC),
		now,
		48*time.Hour,
	)

	require.False(t, ok)
}

func TestClampOptimizeRangeClampsWeeklyChunkToDelayCutoff(t *testing.T) {
	allowed := optimizeRangeChunk{
		from: time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC),
		to:   time.Date(2026, time.March, 29, 0, 0, 0, 0, time.UTC),
	}

	chunk, ok := clampOptimizeRange(
		optimizeChunkForDate(time.Date(2026, time.March, 29, 0, 0, 0, 0, time.UTC), optimizeChunkWeek),
		allowed,
	)

	require.True(t, ok)
	require.Equal(t, time.Date(2026, time.March, 23, 0, 0, 0, 0, time.UTC), chunk.from)
	require.Equal(t, time.Date(2026, time.March, 29, 0, 0, 0, 0, time.UTC), chunk.to)
}

func TestClampOptimizeRangeClampsMonthlyChunkToDelayCutoff(t *testing.T) {
	allowed := optimizeRangeChunk{
		from: time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC),
		to:   time.Date(2026, time.March, 29, 0, 0, 0, 0, time.UTC),
	}

	chunk, ok := clampOptimizeRange(
		optimizeChunkForDate(time.Date(2026, time.March, 15, 0, 0, 0, 0, time.UTC), optimizeChunkMonth),
		allowed,
	)

	require.True(t, ok)
	require.Equal(t, time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC), chunk.from)
	require.Equal(t, time.Date(2026, time.March, 29, 0, 0, 0, 0, time.UTC), chunk.to)
}
