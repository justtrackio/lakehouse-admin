package internal

import (
	"strings"
	"testing"
	"time"
)

func TestBuildOptimizeApplicationNameIncludesTaskID(t *testing.T) {
	name := buildOptimizeApplicationName("analytics.orders", time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC), 4711)

	if !strings.HasSuffix(name, "2026-03-16-4711") {
		t.Fatalf("expected application name to include date and task id suffix, got %q", name)
	}

	if len(name) > sparkApplicationNameMaxLength {
		t.Fatalf("expected application name to be <= %d chars, got %d (%q)", sparkApplicationNameMaxLength, len(name), name)
	}
}

func TestBuildOptimizeApplicationNamePreservesTaskIDSuffixWhenTableIsLong(t *testing.T) {
	name := buildOptimizeApplicationName("very-long-table-name-with-many-segments-and-extra-characters-for-testing", time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC), 987654321)

	if !strings.HasSuffix(name, "2026-03-16-987654321") {
		t.Fatalf("expected long-name application to preserve task id suffix, got %q", name)
	}

	if len(name) > sparkApplicationNameMaxLength {
		t.Fatalf("expected long-name application to be <= %d chars, got %d (%q)", sparkApplicationNameMaxLength, len(name), name)
	}
}

func TestBuildOptimizeApplicationNameFallsBackWithoutTablePart(t *testing.T) {
	name := buildOptimizeApplicationName("!!!", time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC), 99)

	if name != "rewrite-data-files-2026-03-16-99" {
		t.Fatalf("unexpected fallback application name %q", name)
	}
}
