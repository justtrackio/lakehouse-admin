package main

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// DateTime is a thin wrapper around time.Time with custom JSON parsing.
//
// It accepts either:
//   - a date-only string in the form "2006-01-02" (interpreted as UTC midnight)
//   - a full RFC3339 / RFC3339Nano timestamp
//   - null
//
// This is handy for APIs that sometimes send only a date.
type DateTime struct {
	time.Time
}

const dateOnlyLayout = "2006-01-02"

func (d *DateTime) UnmarshalJSON(b []byte) error {
	// allow null
	if string(b) == "null" {
		d.Time = time.Time{}
		return nil
	}

	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	s = strings.TrimSpace(s)
	if s == "" {
		d.Time = time.Time{}
		return nil
	}

	// date-only
	if len(s) == len(dateOnlyLayout) {
		if t, err := time.ParseInLocation(dateOnlyLayout, s, time.UTC); err == nil {
			d.Time = t
			return nil
		}
	}

	// RFC3339 (try nano first, then standard)
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		d.Time = t
		return nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		d.Time = t
		return nil
	}

	return fmt.Errorf("invalid datetime %q (expected %s or RFC3339)", s, dateOnlyLayout)
}

func (d DateTime) MarshalJSON() ([]byte, error) {
	if d.Time.IsZero() {
		return []byte("null"), nil
	}
	// Always emit RFC3339 for stability.
	return json.Marshal(d.Time.Format(time.RFC3339Nano))
}

// Value implements driver.Valuer so DateTime can be stored via database/sql.
func (d DateTime) Value() (driver.Value, error) {
	if d.Time.IsZero() {
		return nil, nil
	}
	return d.Time, nil
}

// Scan implements sql.Scanner so DateTime can be read via database/sql.
func (d *DateTime) Scan(src any) error {
	if src == nil {
		d.Time = time.Time{}
		return nil
	}

	switch v := src.(type) {
	case time.Time:
		d.Time = v
		return nil
	case []byte:
		return d.parseString(string(v))
	case string:
		return d.parseString(v)
	default:
		return errors.New("unsupported Scan type for DateTime")
	}
}

func (d *DateTime) parseString(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		d.Time = time.Time{}
		return nil
	}

	if len(s) == len(dateOnlyLayout) {
		if t, err := time.ParseInLocation(dateOnlyLayout, s, time.UTC); err == nil {
			d.Time = t
			return nil
		}
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		d.Time = t
		return nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		d.Time = t
		return nil
	}

	return fmt.Errorf("invalid datetime %q", s)
}

