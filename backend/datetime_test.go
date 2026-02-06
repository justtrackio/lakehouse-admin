package main

import (
	"database/sql/driver"
	"encoding/json"
	"testing"
	"time"
)

func TestDateTime_UnmarshalJSON_DateOnly(t *testing.T) {
	var d DateTime
	if err := json.Unmarshal([]byte(`"2026-01-01"`), &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if d.IsZero() {
		t.Fatalf("expected non-zero")
	}
	// Date-only is interpreted as UTC midnight.
	want := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !d.Time.Equal(want) {
		t.Fatalf("got %v want %v", d.Time, want)
	}
}

func TestDateTime_UnmarshalJSON_RFC3339(t *testing.T) {
	var d DateTime
	if err := json.Unmarshal([]byte(`"2026-01-01T12:34:56Z"`), &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if d.Time.UTC().Format(time.RFC3339) != "2026-01-01T12:34:56Z" {
		t.Fatalf("unexpected time: %v", d.Time)
	}
}

func TestDateTime_UnmarshalJSON_Null(t *testing.T) {
	var d DateTime
	if err := json.Unmarshal([]byte(`null`), &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !d.IsZero() {
		t.Fatalf("expected zero")
	}
}

func TestDateTime_MarshalJSON(t *testing.T) {
	d := DateTime{Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `"2026-01-01T00:00:00Z"` {
		t.Fatalf("got %s", string(b))
	}
}

func TestDateTime_Value(t *testing.T) {
	d := DateTime{}
	v, err := d.Value()
	if err != nil {
		t.Fatalf("value: %v", err)
	}
	if v != nil {
		t.Fatalf("expected nil")
	}

	d = DateTime{Time: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	v, err = d.Value()
	if err != nil {
		t.Fatalf("value: %v", err)
	}
	if _, ok := v.(time.Time); !ok {
		t.Fatalf("expected time.Time, got %T", v)
	}
}

func TestDateTime_Scan(t *testing.T) {
	var d DateTime
	if err := d.Scan(nil); err != nil {
		t.Fatalf("scan nil: %v", err)
	}
	if !d.IsZero() {
		t.Fatalf("expected zero")
	}

	tm := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := d.Scan(tm); err != nil {
		t.Fatalf("scan time: %v", err)
	}
	if !d.Time.Equal(tm) {
		t.Fatalf("got %v want %v", d.Time, tm)
	}

	if err := d.Scan("2026-01-01"); err != nil {
		t.Fatalf("scan date string: %v", err)
	}
	if !d.Time.Equal(tm) {
		t.Fatalf("got %v want %v", d.Time, tm)
	}

	// driver.Value can be []byte
	if err := d.Scan(driver.Value([]byte("2026-01-01T00:00:00Z"))); err != nil {
		t.Fatalf("scan bytes: %v", err)
	}
	if d.Time.UTC().Format(time.RFC3339) != "2026-01-01T00:00:00Z" {
		t.Fatalf("unexpected time: %v", d.Time)
	}
}
