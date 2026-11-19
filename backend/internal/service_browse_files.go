package internal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

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
	var selections []browseFileSelection
	var files []IcebergDataFile

	if table, err = s.metadata.GetTable(ctx, database, tableName); err != nil {
		return nil, fmt.Errorf("could not load table metadata for files browse: %w", err)
	}

	if selections, err = s.resolveBrowseFileSelections(table.Partitions.Get(), filters); err != nil {
		return nil, err
	}

	if files, err = s.iceberg.ListDataFiles(ctx, table.Database, tableName); err != nil {
		return nil, fmt.Errorf("could not list data files: %w", err)
	}

	return filterBrowseFiles(files, selections), nil
}

// filterBrowseFiles keeps the files whose partition matches every selection,
// largest first.
func filterBrowseFiles(files []IcebergDataFile, selections []browseFileSelection) []DataFileItem {
	items := make([]DataFileItem, 0)

	for _, file := range files {
		if !matchesBrowseSelections(file.Partition, selections) {
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

func matchesBrowseSelections(partition []IcebergPartitionField, selections []browseFileSelection) bool {
	values := make(map[string]string, len(partition))
	for _, field := range partition {
		if !field.Null {
			values[field.Name] = field.Value
		}
	}

	for _, selection := range selections {
		if value, ok := values[selection.RawFieldName]; !ok || value != selection.Value {
			return false
		}
	}

	return true
}

func formatBrowsePartition(partition []IcebergPartitionField) string {
	parts := make([]string, len(partition))
	for i, field := range partition {
		parts[i] = field.Name + "=" + field.Value
	}

	return "{" + strings.Join(parts, ", ") + "}"
}

type browseFileSelection struct {
	RawFieldName string
	Value        string
}

func (s *ServiceBrowseFiles) resolveBrowseFileSelections(fields []TablePartition, filters map[string]string) ([]browseFileSelection, error) {
	var err error
	var selection browseFileSelection

	if len(fields) == 0 {
		return nil, newBrowseInputError("table does not define any partitions")
	}

	validFilterKeys := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		validFilterKeys[field.Name] = struct{}{}
	}

	for key := range filters {
		if _, ok := validFilterKeys[key]; !ok {
			return nil, newBrowseInputError("unknown partition key %q", key)
		}
	}

	if len(filters) != len(fields) {
		return nil, newBrowseInputError("listing data files requires a complete partition selection")
	}

	selections := make([]browseFileSelection, 0)
	seenRawFieldNames := make(map[string]struct{}, len(fields))

	for _, field := range fields {
		if field.RawFieldName == "" {
			return nil, newBrowseInputError("partition %q is missing raw field metadata", field.Name)
		}

		if _, seen := seenRawFieldNames[field.RawFieldName]; seen {
			continue
		}

		if selection, err = s.buildBrowseFileSelection(filters, field); err != nil {
			return nil, err
		}

		selections = append(selections, selection)
		seenRawFieldNames[field.RawFieldName] = struct{}{}
	}

	return selections, nil
}

func (s *ServiceBrowseFiles) buildBrowseFileSelection(filters map[string]string, field TablePartition) (browseFileSelection, error) {
	var err error
	var value string

	if !field.IsHidden {
		value, ok := filters[field.Name]
		if !ok {
			return browseFileSelection{}, newBrowseInputError("missing partition filter %q", field.Name)
		}

		return browseFileSelection{RawFieldName: field.RawFieldName, Value: value}, nil
	}

	if value, err = s.buildHiddenPartitionValue(filters, field); err != nil {
		return browseFileSelection{}, err
	}

	return browseFileSelection{RawFieldName: field.RawFieldName, Value: value}, nil
}

func (s *ServiceBrowseFiles) buildHiddenPartitionValue(filters map[string]string, field TablePartition) (string, error) {
	var err error
	var year, month, day string

	switch field.Hidden.Type {
	case transformDay:
		if year, err = s.requireBrowseFilter(filters, "year"); err != nil {
			return "", err
		}

		if month, err = s.requireBrowseFilter(filters, "month"); err != nil {
			return "", err
		}

		if day, err = s.requireBrowseFilter(filters, "day"); err != nil {
			return "", err
		}

		return fmt.Sprintf("%s-%s-%s", year, month, day), nil
	case transformMonth:
		if year, err = s.requireBrowseFilter(filters, "year"); err != nil {
			return "", err
		}

		if month, err = s.requireBrowseFilter(filters, "month"); err != nil {
			return "", err
		}

		return fmt.Sprintf("%s-%s-01", year, month), nil
	case transformYear:
		if year, err = s.requireBrowseFilter(filters, "year"); err != nil {
			return "", err
		}

		return fmt.Sprintf("%s-01-01", year), nil
	default:
		return "", newBrowseInputError("unsupported hidden partition transform %q", field.Hidden.Type)
	}
}

func (s *ServiceBrowseFiles) requireBrowseFilter(filters map[string]string, key string) (string, error) {
	value, ok := filters[key]
	if !ok || value == "" {
		return "", newBrowseInputError("missing partition filter %q", key)
	}

	return value, nil
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
