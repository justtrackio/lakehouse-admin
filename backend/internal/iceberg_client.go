package internal

import (
	"context"
	"fmt"
	"sort"
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

const (
	transformDay   = "day"
	transformMonth = "month"
	transformYear  = "year"
)

type IcebergSettings struct {
	Catalog            string        `cfg:"catalog" default:"lakehouse"`
	DefaultDatabase    string        `cfg:"default_database" default:"main"`
	NeedsOptimizeDelay time.Duration `cfg:"needs_optimize_delay" default:"24h"`
}

func ReadIcebergSettings(config cfg.Config) (*IcebergSettings, error) {
	settings := &IcebergSettings{}
	if err := config.UnmarshalKey("iceberg", settings); err != nil {
		return nil, fmt.Errorf("could not unmarshal iceberg settings: %w", err)
	}

	if settings.DefaultDatabase == "" {
		if legacyDatabase, _ := config.GetString("iceberg.database"); legacyDatabase != "" {
			settings.DefaultDatabase = legacyDatabase
		}
	}

	return settings, nil
}

type icebergCtxKey struct{}

func ProvideIcebergClient(ctx context.Context, config cfg.Config, logger log.Logger) (*IcebergClient, error) {
	return appctx.Provide(ctx, icebergCtxKey{}, func() (*IcebergClient, error) {
		var err error
		var awsCfg aws.Config
		var settings *IcebergSettings

		if settings, err = ReadIcebergSettings(config); err != nil {
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

func (c *IcebergClient) LoadTable(ctx context.Context, database string, logicalName string) (*table.Table, error) {
	identifier := c.resolveTableIdentifier(database, logicalName)

	ctx = utils.WithAwsConfig(ctx, &c.awsCfg)
	tbl, err := c.catalog.LoadTable(ctx, identifier)
	if err != nil {
		return nil, fmt.Errorf("could not load table %s: %w", identifier, err)
	}

	return tbl, nil
}

func (c *IcebergClient) resolveTableIdentifier(database string, logicalName string) table.Identifier {
	if strings.Contains(logicalName, ".") {
		parts := strings.Split(logicalName, ".")

		return parts
	}

	if database == "" {
		database = c.settings.DefaultDatabase
	}

	return []string{database, logicalName}
}

func (c *IcebergClient) ListSnapshots(ctx context.Context, database string, logicalName string) ([]table.Snapshot, error) {
	tbl, err := c.LoadTable(ctx, database, logicalName)
	if err != nil {
		return nil, fmt.Errorf("could not load table: %w", err)
	}

	metadata := tbl.Metadata()
	snapshots := metadata.Snapshots()

	return snapshots, nil
}

func (c *IcebergClient) ListSnapshotDataFilePaths(ctx context.Context, database string, logicalName string, snapshotID int64) ([]string, error) {
	tbl, err := c.LoadTable(ctx, database, logicalName)
	if err != nil {
		return nil, fmt.Errorf("could not load table: %w", err)
	}

	return c.listSnapshotDataFilePaths(ctx, tbl, snapshotID)
}

func (c *IcebergClient) listSnapshotDataFilePaths(ctx context.Context, tbl *table.Table, snapshotID int64) ([]string, error) {
	scanner := tbl.Scan(table.WithSnapshotID(snapshotID))

	ctx = utils.WithAwsConfig(ctx, &c.awsCfg)
	tasks, err := scanner.PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not plan files for snapshot %d: %w", snapshotID, err)
	}

	result := make([]string, 0, len(tasks))
	seen := make(map[string]struct{}, len(tasks))

	for _, task := range tasks {
		path := task.File.FilePath()
		if _, ok := seen[path]; ok {
			continue
		}

		seen[path] = struct{}{}
		result = append(result, path)
	}

	sort.Strings(result)

	return result, nil
}

// ListPartitions returns partition stats with browse-compatible keys
// that match the TableDescription.Partitions names (year, month, day for time transforms,
// or column name for identity transforms).
func (c *IcebergClient) ListPartitions(ctx context.Context, database string, logicalName string) ([]IcebergPartitionStats, error) {
	tbl, err := c.LoadTable(ctx, database, logicalName)
	if err != nil {
		return nil, fmt.Errorf("could not load table: %w", err)
	}

	currentSnapshot := tbl.CurrentSnapshot()
	if currentSnapshot == nil {
		return []IcebergPartitionStats{}, nil
	}

	metadata := tbl.Metadata()
	spec := c.getDefaultPartitionSpec(metadata)
	schema := metadata.CurrentSchema()

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
			normalizedPartition := c.normalizePartitionForBrowse(file.Partition(), spec, schema)

			partitionMap[partitionKey] = &IcebergPartitionStats{
				Partition:      normalizedPartition,
				RawPartition:   file.Partition(),
				SpecID:         file.SpecID(),
				RecordCount:    0,
				Files:          make(IcebergPartitionStatsFiles, 0),
				LastUpdatedAt:  currentSnapshot.TimestampMs,
				LastSnapshotID: currentSnapshot.SnapshotID,
			}
		}

		stats := partitionMap[partitionKey]
		stats.RecordCount += file.Count()
		stats.Files = append(stats.Files, IcebergPartitionFileStats{
			SizeBytes: file.FileSizeBytes(),
		})
	}

	result := make([]IcebergPartitionStats, 0, len(partitionMap))
	for _, stats := range partitionMap {
		result = append(result, *stats)
	}

	return result, nil
}

// Removed partitionToMap, but kept partitionKeyString as it is used by ListPartitions
func (c *IcebergClient) partitionKeyString(partition map[int]any) string {
	if len(partition) == 0 {
		return "unpartitioned"
	}

	keys := make([]int, 0, len(partition))
	for k := range partition {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%d=%v", k, partition[k]))
	}

	return strings.Join(parts, "|")
}

func (c *IcebergClient) getDefaultPartitionSpec(metadata table.Metadata) *iceberg.PartitionSpec {
	specs := metadata.PartitionSpecs()
	defaultSpecID := metadata.DefaultPartitionSpec()

	if len(specs) == 0 {
		return nil
	}

	if defaultSpecID >= 0 && defaultSpecID < len(specs) {
		return &specs[defaultSpecID]
	}

	for i := range specs {
		return &specs[i]
	}

	return nil
}

func (c *IcebergClient) normalizePartitionForBrowse(rawPartition map[int]any, spec *iceberg.PartitionSpec, schema *iceberg.Schema) map[string]any {
	result := make(map[string]any)

	if spec == nil || len(rawPartition) == 0 {
		return result
	}

	// Build a map from partition field ID to partition field info
	fieldIDToSpec := make(map[int]iceberg.PartitionField)
	for pf := range spec.Fields() {
		fieldIDToSpec[pf.FieldID] = pf
	}

	for partFieldID, val := range rawPartition {
		pf, ok := fieldIDToSpec[partFieldID]
		if !ok {
			// Unknown partition field, use field_<id> as fallback
			result[fmt.Sprintf("field_%d", partFieldID)] = val

			continue
		}

		sourceColumnName, ok := c.findSourceColumnName(schema, pf.SourceID)
		if !ok {
			result[fmt.Sprintf("field_%d", partFieldID)] = val

			continue
		}

		transform := pf.Transform.String()

		switch transform {
		case "identity":
			result[sourceColumnName] = val
		case transformDay:
			t := val.(iceberg.Date).ToTime()
			result["year"] = t.Format("2006")
			result["month"] = t.Format("01")
			result["day"] = t.Format("02")
		case transformMonth:
			t := val.(iceberg.Date).ToTime()
			result["year"] = t.Format("2006")
			result["month"] = t.Format("01")
		case transformYear:
			t := val.(iceberg.Date).ToTime()
			result["year"] = t.Format("2006")
		default:
			// For other transforms (bucket, truncate), use the partition field name
			result[pf.Name] = val
		}
	}

	return result
}

func (c *IcebergClient) findSourceColumnName(schema *iceberg.Schema, sourceID int) (string, bool) {
	if sourceColumnName, ok := schema.FindColumnName(sourceID); ok {
		return sourceColumnName, true
	}

	sourceField, ok := schema.FindFieldByID(sourceID)
	if !ok {
		return "", false
	}

	return sourceField.Name, true
}

func (c *IcebergClient) ListTables(ctx context.Context, database string) ([]table.Identifier, error) {
	var err error
	var t table.Identifier
	var tables []table.Identifier

	if database == "" {
		database = c.settings.DefaultDatabase
	}

	ctx = utils.WithAwsConfig(ctx, &c.awsCfg)
	iterator := c.catalog.ListTables(ctx, table.Identifier{database})

	for t, err = range iterator {
		if err != nil {
			return nil, fmt.Errorf("error while iterating tables: %w", err)
		}

		tables = append(tables, t)
	}

	return tables, nil
}

func (c *IcebergClient) DescribeTable(ctx context.Context, database string, logicalName string) (*TableDescription, error) {
	tbl, err := c.LoadTable(ctx, database, logicalName)
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
		Database:          database,
		Name:              logicalName,
		Columns:           columns,
		Partitions:        partitions,
		CurrentSnapshotID: nil,
		UpdatedAt:         time.Now(),
	}

	if currentSnapshot := tbl.CurrentSnapshot(); currentSnapshot != nil {
		desc.CurrentSnapshotID = &currentSnapshot.SnapshotID
	}

	return desc, nil
}

func (c *IcebergClient) ListDatabases(ctx context.Context) ([]string, error) {
	ctx = utils.WithAwsConfig(ctx, &c.awsCfg)

	namespaces, err := c.catalog.ListNamespaces(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("could not list namespaces from iceberg catalog: %w", err)
	}

	databases := make([]string, 0, len(namespaces))
	for _, namespace := range namespaces {
		if len(namespace) == 0 {
			continue
		}

		databases = append(databases, namespace[len(namespace)-1])
	}

	sort.Strings(databases)

	return databases, nil
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
	var spec *iceberg.PartitionSpec

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
		sourceColumnName, found := c.findSourceColumnName(schema, pf.SourceID)
		if !found {
			return db.NewJSON(partitions, db.NonNullable{}), fmt.Errorf("could not find source field with id %d for partition field %s", pf.SourceID, pf.Name)
		}

		switch pf.Transform.String() {
		case transformDay, transformMonth, transformYear:
			partitions = append(partitions, c.expandTimeTransform(pf.Transform.String(), sourceColumnName, pf.Name)...)
		case "identity":
			partitions = append(partitions, TablePartition{
				Name:         sourceColumnName,
				RawFieldName: pf.Name,
				IsHidden:     false,
				Hidden:       TablePartitionHidden{},
			})
		default:
			return db.NewJSON(partitions, db.NonNullable{}), fmt.Errorf("unknown partition transformer type: %s", pf.Transform.String())
		}
	}

	return db.NewJSON(partitions, db.NonNullable{}), nil
}

func (c *IcebergClient) expandTimeTransform(transform, sourceCol, rawFieldName string) []TablePartition {
	switch transform {
	case transformDay:
		return []TablePartition{
			{Name: "year", RawFieldName: rawFieldName, IsHidden: true, Hidden: TablePartitionHidden{Column: sourceCol, Type: transformDay}},
			{Name: "month", RawFieldName: rawFieldName, IsHidden: true, Hidden: TablePartitionHidden{Column: sourceCol, Type: transformDay}},
			{Name: "day", RawFieldName: rawFieldName, IsHidden: true, Hidden: TablePartitionHidden{Column: sourceCol, Type: transformDay}},
		}
	case transformMonth:
		return []TablePartition{
			{Name: "year", RawFieldName: rawFieldName, IsHidden: true, Hidden: TablePartitionHidden{Column: sourceCol, Type: transformMonth}},
			{Name: "month", RawFieldName: rawFieldName, IsHidden: true, Hidden: TablePartitionHidden{Column: sourceCol, Type: transformMonth}},
		}
	case transformYear:
		return []TablePartition{
			{Name: "year", RawFieldName: rawFieldName, IsHidden: true, Hidden: TablePartitionHidden{Column: sourceCol, Type: transformYear}},
		}
	default:
		return nil
	}
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
