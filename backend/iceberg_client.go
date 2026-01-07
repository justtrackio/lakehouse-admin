package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	gosoGlue "github.com/justtrackio/gosoline/pkg/cloud/aws/glue"
	"github.com/justtrackio/gosoline/pkg/db"
	"github.com/justtrackio/gosoline/pkg/log"
)

type IcebergSettings struct {
	DefaultDatabase string `cfg:"default_database" default:"main"`
}

type icebergCtxKey struct{}

func ProvideIcebergClient(ctx context.Context, config cfg.Config, logger log.Logger) (*IcebergClient, error) {
	return appctx.Provide(ctx, icebergCtxKey{}, func() (*IcebergClient, error) {
		var err error
		var awsCfg aws.Config

		settings := &IcebergSettings{}
		if err = config.UnmarshalKey("iceberg", settings); err != nil {
			return nil, fmt.Errorf("could not unmarshal iceberg settings: %w", err)
		}

		if _, awsCfg, err = gosoGlue.NewConfig(ctx, config, logger, "default"); err != nil {
			return nil, fmt.Errorf("could not create aws config for iceberg client: %w", err)
		}

		cat := glue.NewCatalog(glue.WithAwsConfig(awsCfg), glue.WithAwsProperties(map[string]string{
			"s3.force-virtual-addressing": "true",
		}))

		return &IcebergClient{
			awsCfg:   awsCfg,
			catalog:  cat,
			settings: settings,
			logger:   logger.WithChannel("iceberg"),
		}, nil
	})
}

type IcebergClient struct {
	awsCfg   aws.Config
	catalog  catalog.Catalog
	settings *IcebergSettings
	logger   log.Logger
}

func (c *IcebergClient) LoadTable(ctx context.Context, logicalName string) (*table.Table, error) {
	identifier := c.resolveTableIdentifier(logicalName)

	ctx = utils.WithAwsConfig(ctx, &c.awsCfg)
	tbl, err := c.catalog.LoadTable(ctx, identifier)
	if err != nil {
		return nil, fmt.Errorf("could not load table %s: %w", identifier, err)
	}

	return tbl, nil
}

func (c *IcebergClient) resolveTableIdentifier(logicalName string) table.Identifier {
	if strings.Contains(logicalName, ".") {
		parts := strings.Split(logicalName, ".")
		return parts
	}

	return []string{c.settings.DefaultDatabase, logicalName}
}

func (c *IcebergClient) ListSnapshots(ctx context.Context, logicalName string) ([]table.Snapshot, error) {
	tbl, err := c.LoadTable(ctx, logicalName)
	if err != nil {
		return nil, fmt.Errorf("could not load table: %w", err)
	}

	metadata := tbl.Metadata()
	snapshots := metadata.Snapshots()

	return snapshots, nil
}

type IcebergPartitionStats struct {
	Partition         map[string]any
	SpecID            int32
	RecordCount       int64
	FileCount         int64
	DataFileSizeBytes int64
	LastUpdatedAt     int64
	LastSnapshotID    int64
}

func (c *IcebergClient) ListPartitions(ctx context.Context, logicalName string) ([]IcebergPartitionStats, error) {
	tbl, err := c.LoadTable(ctx, logicalName)
	if err != nil {
		return nil, fmt.Errorf("could not load table: %w", err)
	}

	currentSnapshot := tbl.CurrentSnapshot()
	if currentSnapshot == nil {
		return []IcebergPartitionStats{}, nil
	}

	partitionMap := make(map[string]*IcebergPartitionStats)

	scanner := tbl.Scan()

	ctx = utils.WithAwsConfig(ctx, &c.awsCfg)
	tasks, err := scanner.PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not plan files: %w", err)
	}

	for _, task := range tasks {
		file := task.File

		partitionKey := c.partitionKeyString(file.Partition())

		if _, exists := partitionMap[partitionKey]; !exists {
			partitionMap[partitionKey] = &IcebergPartitionStats{
				Partition:         c.partitionToMap(file.Partition()),
				SpecID:            file.SpecID(),
				RecordCount:       0,
				FileCount:         0,
				DataFileSizeBytes: 0,
				LastUpdatedAt:     currentSnapshot.TimestampMs,
				LastSnapshotID:    currentSnapshot.SnapshotID,
			}
		}

		stats := partitionMap[partitionKey]
		stats.RecordCount += file.Count()
		stats.FileCount++
		stats.DataFileSizeBytes += file.FileSizeBytes()
	}

	result := make([]IcebergPartitionStats, 0, len(partitionMap))
	for _, stats := range partitionMap {
		result = append(result, *stats)
	}

	return result, nil
}

func (c *IcebergClient) partitionToMap(partition map[int]any) map[string]any {
	result := make(map[string]any)
	if len(partition) == 0 {
		return result
	}

	for fieldID, val := range partition {
		result[fmt.Sprintf("field_%d", fieldID)] = val
	}

	return result
}

func (c *IcebergClient) partitionKeyString(partition map[int]any) string {
	if len(partition) == 0 {
		return "unpartitioned"
	}

	parts := make([]string, 0, len(partition))
	for fieldID, val := range partition {
		parts = append(parts, fmt.Sprintf("%d=%v", fieldID, val))
	}

	return strings.Join(parts, "|")
}

func (c *IcebergClient) ListTables(ctx context.Context) ([]table.Identifier, error) {
	var err error
	var t table.Identifier
	var tables []table.Identifier

	ctx = utils.WithAwsConfig(ctx, &c.awsCfg)
	iterator := c.catalog.ListTables(ctx, table.Identifier{c.settings.DefaultDatabase})

	for t, err = range iterator {
		if err != nil {
			return nil, fmt.Errorf("error while iterating tables: %w", err)
		}

		tables = append(tables, t)
	}

	return tables, nil
}

func (c *IcebergClient) DescribeTable(ctx context.Context, logicalName string) (*TableDescription, error) {
	tbl, err := c.LoadTable(ctx, logicalName)
	if err != nil {
		return nil, fmt.Errorf("could not load table: %w", err)
	}

	metadata := tbl.Metadata()

	columns, err := c.extractColumns(metadata.CurrentSchema())
	if err != nil {
		return nil, fmt.Errorf("could not extract columns: %w", err)
	}

	partitions, err := c.extractPartitions(metadata)
	if err != nil {
		return nil, fmt.Errorf("could not extract partitions: %w", err)
	}

	desc := &TableDescription{
		Name:       logicalName,
		Columns:    columns,
		Partitions: partitions,
		UpdatedAt:  time.Now(),
	}

	return desc, nil
}

func (c *IcebergClient) extractColumns(schema *iceberg.Schema) (db.JSON[TableColumns, db.NonNullable], error) {
	fields := schema.Fields()
	columns := make([]TableColumn, 0, len(fields))

	for _, field := range fields {
		columns = append(columns, TableColumn{
			Name: field.Name,
			Type: c.formatType(field.Type),
		})
	}

	return db.NewJSON(TableColumns(columns), db.NonNullable{}), nil
}

func (c *IcebergClient) extractPartitions(metadata table.Metadata) (db.JSON[[]TablePartition, db.NonNullable], error) {
	var ok bool
	var spec *iceberg.PartitionSpec
	var sourceField iceberg.NestedField

	specs := metadata.PartitionSpecs()
	defaultSpecID := metadata.DefaultPartitionSpec()

	if len(specs) == 0 {
		return db.NewJSON([]TablePartition{}, db.NonNullable{}), nil
	}

	if defaultSpecID >= 0 && defaultSpecID < len(specs) {
		spec = &specs[defaultSpecID]
	}

	if spec == nil {
		for i := range specs {
			spec = &specs[i]
			break
		}
	}

	if spec == nil {
		return db.NewJSON([]TablePartition{}, db.NonNullable{}), nil
	}

	partitions := make([]TablePartition, 0)
	fields := spec.Fields()
	schema := metadata.CurrentSchema()

	for pf := range fields {
		if sourceField, ok = schema.FindFieldByID(pf.SourceID); !ok {
			return db.NewJSON(partitions, db.NonNullable{}), fmt.Errorf("could not find source field with id %d for partition field %s", pf.SourceID, pf.Name)
		}

		switch pf.Transform.String() {
		case "day", "month", "year":
			partitions = append(partitions, c.expandTimeTransform(pf.Transform.String(), sourceField.Name)...)
		case "identity":
			partitions = append(partitions, TablePartition{
				Name:     sourceField.Name,
				IsHidden: false,
				Hidden:   TablePartitionHidden{},
			})
		default:
			return db.NewJSON(partitions, db.NonNullable{}), fmt.Errorf("unknown partition transformer type: %s", pf.Transform.String())
		}
	}

	return db.NewJSON(partitions, db.NonNullable{}), nil
}

func (c *IcebergClient) expandTimeTransform(transform, sourceCol string) []TablePartition {
	switch transform {
	case "day":
		return []TablePartition{
			{Name: "year", IsHidden: true, Hidden: TablePartitionHidden{Column: sourceCol, Type: "day"}},
			{Name: "month", IsHidden: true, Hidden: TablePartitionHidden{Column: sourceCol, Type: "day"}},
			{Name: "day", IsHidden: true, Hidden: TablePartitionHidden{Column: sourceCol, Type: "day"}},
		}
	case "month":
		return []TablePartition{
			{Name: "year", IsHidden: true, Hidden: TablePartitionHidden{Column: sourceCol, Type: "month"}},
			{Name: "month", IsHidden: true, Hidden: TablePartitionHidden{Column: sourceCol, Type: "month"}},
		}
	case "year":
		return []TablePartition{
			{Name: "year", IsHidden: true, Hidden: TablePartitionHidden{Column: sourceCol, Type: "year"}},
		}
	}
	return nil
}

func (c *IcebergClient) formatType(t iceberg.Type) string {
	typeStr := t.String()

	if !strings.HasPrefix(typeStr, "struct<") && !strings.HasPrefix(typeStr, "list<") && !strings.HasPrefix(typeStr, "map<") {
		return typeStr
	}

	switch v := t.(type) {
	case *iceberg.StructType:
		return c.formatStruct(v)
	case *iceberg.ListType:
		return c.formatList(v)
	case *iceberg.MapType:
		return c.formatMap(v)
	default:
		return typeStr
	}
}

func (c *IcebergClient) formatStruct(t *iceberg.StructType) string {
	fieldList := t.FieldList
	if len(fieldList) == 0 {
		return "struct<>"
	}

	fields := make([]string, 0, len(fieldList))
	for _, field := range fieldList {
		fields = append(fields, fmt.Sprintf("%s:%s", field.Name, c.formatType(field.Type)))
	}

	return fmt.Sprintf("struct<%s>", strings.Join(fields, ","))
}

func (c *IcebergClient) formatList(t *iceberg.ListType) string {
	return fmt.Sprintf("array<%s>", c.formatType(t.Element))
}

func (c *IcebergClient) formatMap(t *iceberg.MapType) string {
	return fmt.Sprintf("map<%s,%s>", c.formatType(t.KeyType), c.formatType(t.ValueType))
}
