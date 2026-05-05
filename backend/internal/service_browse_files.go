package internal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/spf13/cast"
)

func NewServiceBrowseFiles(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceBrowseFiles, error) {
	var err error
	var metadata *ServiceMetadata
	var trino *TrinoClient

	if metadata, err = NewServiceMetadata(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create service metadata: %w", err)
	}

	if trino, err = ProvideTrinoClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create trino client: %w", err)
	}

	return &ServiceBrowseFiles{
		metadata: metadata,
		trino:    trino,
	}, nil
}

type ServiceBrowseFiles struct {
	metadata *ServiceMetadata
	trino    *TrinoClient
}

func (s *ServiceBrowseFiles) ListFiles(ctx context.Context, database string, tableName string, filters map[string]string) ([]DataFileItem, error) {
	table, err := s.metadata.GetTable(ctx, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("could not load table metadata for files browse: %w", err)
	}

	selections, err := s.resolveBrowseFileSelections(table.Partitions.Get(), filters)
	if err != nil {
		return nil, err
	}

	partitionFieldNames := s.browseSelectionFieldNames(selections)

	rows, err := s.trino.QueryRows(ctx, s.buildBrowseFilesQuery(table.Database, tableName, selections))
	if err != nil {
		return nil, fmt.Errorf("could not query data files from trino: %w", err)
	}

	items := make([]DataFileItem, 0, len(rows))
	for _, row := range rows {
		item, err := s.mapBrowseFileRow(row, partitionFieldNames)
		if err != nil {
			return nil, fmt.Errorf("could not map data file row: %w", err)
		}

		items = append(items, item)
	}

	return items, nil
}

type browseFileSelection struct {
	RawFieldName string
	Value        string
}

func (s *ServiceBrowseFiles) resolveBrowseFileSelections(fields []TablePartition, filters map[string]string) ([]browseFileSelection, error) {
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

		selection, err := s.buildBrowseFileSelection(filters, field)
		if err != nil {
			return nil, err
		}

		selections = append(selections, selection)
		seenRawFieldNames[field.RawFieldName] = struct{}{}
	}

	return selections, nil
}

func (s *ServiceBrowseFiles) buildBrowseFileSelection(filters map[string]string, field TablePartition) (browseFileSelection, error) {
	if !field.IsHidden {
		value, ok := filters[field.Name]
		if !ok {
			return browseFileSelection{}, newBrowseInputError("missing partition filter %q", field.Name)
		}

		return browseFileSelection{RawFieldName: field.RawFieldName, Value: value}, nil
	}

	value, err := s.buildHiddenPartitionValue(filters, field)
	if err != nil {
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

func (s *ServiceBrowseFiles) buildBrowseFilesQuery(database string, table string, selections []browseFileSelection) string {
	qualifiedTable := qualifiedTableName("lakehouse", database, table+"$files")
	query := fmt.Sprintf(`
		SELECT
			content,
			file_path,
			file_format,
			spec_id,
			partition,
			record_count,
			file_size_in_bytes
		FROM %s
		WHERE content = 0
	`, qualifiedTable)

	for _, selection := range selections {
		query += fmt.Sprintf(" AND CAST(partition.%s AS VARCHAR) = %s", quoteIdent(selection.RawFieldName), quoteLiteral(selection.Value))
	}

	query += " ORDER BY file_size_in_bytes DESC, file_path ASC"

	return query
}

func (s *ServiceBrowseFiles) browseSelectionFieldNames(selections []browseFileSelection) []string {
	fieldNames := make([]string, len(selections))
	for i, selection := range selections {
		fieldNames[i] = selection.RawFieldName
	}

	return fieldNames
}

func (s *ServiceBrowseFiles) mapBrowseFileRow(row map[string]any, partitionFieldNames []string) (DataFileItem, error) {
	var err error
	item := DataFileItem{}

	if item.Content, err = cast.ToInt64E(row["content"]); err != nil {
		return DataFileItem{}, fmt.Errorf("could not cast content: %w", err)
	}

	if item.SpecID, err = cast.ToInt64E(row["spec_id"]); err != nil {
		return DataFileItem{}, fmt.Errorf("could not cast spec_id: %w", err)
	}

	if item.RecordCount, err = cast.ToInt64E(row["record_count"]); err != nil {
		return DataFileItem{}, fmt.Errorf("could not cast record_count: %w", err)
	}

	if item.FileSizeInBytes, err = cast.ToInt64E(row["file_size_in_bytes"]); err != nil {
		return DataFileItem{}, fmt.Errorf("could not cast file_size_in_bytes: %w", err)
	}

	if item.FilePath, err = cast.ToStringE(row["file_path"]); err != nil {
		return DataFileItem{}, fmt.Errorf("could not cast file_path: %w", err)
	}

	if item.FileFormat, err = cast.ToStringE(row["file_format"]); err != nil {
		return DataFileItem{}, fmt.Errorf("could not cast file_format: %w", err)
	}

	if item.Partition, err = s.browseRowValueToString(row["partition"], partitionFieldNames); err != nil {
		return DataFileItem{}, fmt.Errorf("could not cast partition: %w", err)
	}

	return item, nil
}

func (s *ServiceBrowseFiles) browseRowValueToString(value any, partitionFieldNames []string) (string, error) {
	switch v := value.(type) {
	case nil:
		return "", nil
	case []byte:
		return string(v), nil
	case []any:
		return s.formatBrowsePartitionTuple(v, partitionFieldNames)
	case map[string]any:
		return s.formatBrowsePartitionMap(v, partitionFieldNames)
	default:
		if s, err := cast.ToStringE(v); err == nil {
			return s, nil
		}

		return fmt.Sprintf("%v", v), nil
	}
}

func (s *ServiceBrowseFiles) formatBrowsePartitionTuple(values []any, partitionFieldNames []string) (string, error) {
	parts := make([]string, 0, len(values))
	for i, value := range values {
		fieldName := fmt.Sprintf("field_%d", i)
		if i < len(partitionFieldNames) && partitionFieldNames[i] != "" {
			fieldName = partitionFieldNames[i]
		}

		formattedValue, err := s.formatBrowsePartitionValue(value)
		if err != nil {
			return "", err
		}

		parts = append(parts, fmt.Sprintf("%s=%s", fieldName, formattedValue))
	}

	return "{" + strings.Join(parts, ", ") + "}", nil
}

func (s *ServiceBrowseFiles) formatBrowsePartitionMap(values map[string]any, partitionFieldNames []string) (string, error) {
	orderedKeys := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))

	for _, key := range partitionFieldNames {
		if _, ok := values[key]; ok {
			orderedKeys = append(orderedKeys, key)
			seen[key] = struct{}{}
		}
	}

	remainingKeys := make([]string, 0, len(values)-len(orderedKeys))
	for key := range values {
		if _, ok := seen[key]; ok {
			continue
		}

		remainingKeys = append(remainingKeys, key)
	}
	sort.Strings(remainingKeys)
	orderedKeys = append(orderedKeys, remainingKeys...)

	parts := make([]string, 0, len(orderedKeys))
	for _, key := range orderedKeys {
		formattedValue, err := s.formatBrowsePartitionValue(values[key])
		if err != nil {
			return "", err
		}

		parts = append(parts, fmt.Sprintf("%s=%s", key, formattedValue))
	}

	return "{" + strings.Join(parts, ", ") + "}", nil
}

func (s *ServiceBrowseFiles) formatBrowsePartitionValue(value any) (string, error) {
	switch v := value.(type) {
	case nil:
		return "null", nil
	case []byte:
		return string(v), nil
	default:
		if s, err := cast.ToStringE(v); err == nil {
			return s, nil
		}

		return fmt.Sprintf("%v", v), nil
	}
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
