package internal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewServiceBrowseFiles(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceBrowseFiles, error) {
	var err error
	var metadata *ServiceMetadata
	var iceberg *IcebergClient

	if metadata, err = NewServiceMetadata(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create service metadata: %w", err)
	}

	if iceberg, err = ProvideIcebergClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create iceberg client: %w", err)
	}

	return &ServiceBrowseFiles{
		metadata: metadata,
		iceberg:  iceberg,
	}, nil
}

type ServiceBrowseFiles struct {
	metadata *ServiceMetadata
	iceberg  *IcebergClient
}

func (s *ServiceBrowseFiles) ListFiles(ctx context.Context, database string, tableName string, filters map[string]string) ([]DataFileItem, error) {
	var err error
	var table *TableDescription
	var files []IcebergDataFile

	if table, err = s.metadata.GetTable(ctx, database, tableName); err != nil {
		return nil, fmt.Errorf("could not load table metadata for files browse: %w", err)
	}

	if err := validateBrowseFilters(table.Partitions.Get(), filters); err != nil {
		return nil, err
	}

	if files, err = s.iceberg.ListDataFiles(ctx, table.Database, tableName); err != nil {
		return nil, fmt.Errorf("could not list data files: %w", err)
	}

	return filterBrowseFiles(files, filters), nil
}

// validateBrowseFilters requires one value for every browse key of the table.
func validateBrowseFilters(fields []TablePartition, filters map[string]string) error {
	if len(fields) == 0 {
		return newBrowseInputError("table does not define any partitions")
	}

	keys := make(map[string]bool, len(fields))
	for _, field := range fields {
		keys[field.Name] = true
	}

	for key := range filters {
		if !keys[key] {
			return newBrowseInputError("unknown partition key %q", key)
		}
	}

	if len(filters) != len(keys) {
		return newBrowseInputError("listing data files requires a complete partition selection")
	}

	return nil
}

// filterBrowseFiles keeps the files whose browse values match every filter,
// largest first.
func filterBrowseFiles(files []IcebergDataFile, filters map[string]string) []DataFileItem {
	items := make([]DataFileItem, 0)

	for _, file := range files {
		if !matchesBrowseFilters(file.Values, filters) {
			continue
		}

		items = append(items, DataFileItem{
			Content:         0,
			FilePath:        file.FilePath,
			FileFormat:      file.FileFormat,
			SpecID:          file.SpecID,
			Partition:       formatBrowsePartition(file.Partition),
			RecordCount:     file.RecordCount,
			FileSizeInBytes: file.FileSizeInBytes,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].FileSizeInBytes != items[j].FileSizeInBytes {
			return items[i].FileSizeInBytes > items[j].FileSizeInBytes
		}

		return items[i].FilePath < items[j].FilePath
	})

	return items
}

// matchesBrowseFilters compares the text of each browse value with its filter,
// the text the partition tree reads from the stored values. Null never matches.
func matchesBrowseFilters(values PartitionValues, filters map[string]string) bool {
	for key, filter := range filters {
		value, ok := values[key]
		if !ok || value == nil {
			return false
		}

		if fmt.Sprintf("%v", value) == filter {
			continue
		}

		if date, isDate := value.(identityDate); !isDate || !isLegacyDateFilter(string(date), filter) {
			return false
		}
	}

	return true
}

// isLegacyDateFilter matches an identity date against days since 1970-01-01,
// the form refresh stored identity dates in before the next refresh rewrites
// them.
func isLegacyDateFilter(value string, filter string) bool {
	var err error
	var days int

	if days, err = strconv.Atoi(filter); err != nil {
		return false
	}

	date := time.Unix(0, 0).UTC().AddDate(0, 0, days)

	return date.Format(time.DateOnly) == value
}

func formatBrowsePartition(partition []IcebergPartitionField) string {
	parts := make([]string, len(partition))
	for i, field := range partition {
		parts[i] = field.Name + "=" + field.Value
	}

	return "{" + strings.Join(parts, ", ") + "}"
}

type BrowseInputError struct {
	err error
}

func (e *BrowseInputError) Error() string {
	return e.err.Error()
}

func (e *BrowseInputError) Unwrap() error {
	return e.err
}

func newBrowseInputError(format string, args ...any) error {
	return &BrowseInputError{err: fmt.Errorf(format, args...)}
}

func isBrowseInputError(err error) bool {
	var target *BrowseInputError

	return errors.As(err, &target)
}
