package internal

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	icebergio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud/s3" // registers the s3:// file io
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	gosoGlue "github.com/justtrackio/gosoline/pkg/cloud/aws/glue"
	"github.com/justtrackio/gosoline/pkg/db"
	"github.com/justtrackio/gosoline/pkg/log"
	"golang.org/x/sync/errgroup"
)

const manifestReadConcurrency = 8

const (
	transformDay   = "day"
	transformMonth = "month"
	transformYear  = "year"
)

type IcebergSettings struct {
	Catalog            string        `cfg:"catalog" default:"lakehouse"`
	DefaultDatabase    string        `cfg:"default_database" default:"main"`
	Databases          []string      `cfg:"databases"`
	NeedsOptimizeDelay time.Duration `cfg:"needs_optimize_delay" default:"24h"`
}

func ReadIcebergSettings(config cfg.Config) (*IcebergSettings, error) {
	settings := &IcebergSettings{}
	if err := config.UnmarshalKey("iceberg", settings); err != nil {
		return nil, fmt.Errorf("could not unmarshal iceberg settings: %w", err)
	}

	if settings.DefaultDatabase == "" {
		if legacyDatabase, err := config.GetString("iceberg.database"); err == nil && legacyDatabase != "" {
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

		cat, err := glue.NewCatalog(glue.WithAwsConfig(awsCfg), glue.WithAwsProperties(map[string]string{
			"s3.force-virtual-addressing": "true",
		}))
		if err != nil {
			return nil, fmt.Errorf("could not create glue catalog: %w", err)
		}

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
	var err error
	var tbl *table.Table

	identifier := c.resolveTableIdentifier(database, logicalName)

	ctx = utils.WithAwsConfig(ctx, &c.awsCfg)
	if tbl, err = c.catalog.LoadTable(ctx, identifier); err != nil {
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
	var err error
	var tbl *table.Table

	if tbl, err = c.LoadTable(ctx, database, logicalName); err != nil {
		return nil, fmt.Errorf("could not load table: %w", err)
	}

	metadata := tbl.Metadata()
	snapshots := metadata.Snapshots()

	return snapshots, nil
}

func (c *IcebergClient) ListSnapshotDataFilePaths(ctx context.Context, database string, logicalName string, snapshotID int64) ([]string, error) {
	var err error
	var tbl *table.Table

	if tbl, err = c.LoadTable(ctx, database, logicalName); err != nil {
		return nil, fmt.Errorf("could not load table: %w", err)
	}

	return c.listSnapshotDataFilePaths(ctx, tbl, snapshotID)
}

func (c *IcebergClient) listSnapshotDataFilePaths(ctx context.Context, tbl *table.Table, snapshotID int64) ([]string, error) {
	var err error
	var tasks []table.FileScanTask

	scanner := tbl.Scan(table.WithSnapshotID(snapshotID))

	ctx = utils.WithAwsConfig(ctx, &c.awsCfg)
	if tasks, err = scanner.PlanFiles(ctx); err != nil {
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

// ListDataFiles returns the live data files of the table's current snapshot.
func (c *IcebergClient) ListDataFiles(ctx context.Context, database string, logicalName string) ([]IcebergDataFile, error) {
	var err error
	var tbl *table.Table
	var files []iceberg.DataFile

	if tbl, err = c.LoadTable(ctx, database, logicalName); err != nil {
		return nil, fmt.Errorf("could not load table: %w", err)
	}

	if files, err = c.liveDataFiles(ctx, tbl); err != nil {
		return nil, err
	}

	specs := partitionSpecsByID(tbl.Metadata())
	schema := tbl.Metadata().CurrentSchema()

	result := make([]IcebergDataFile, 0, len(files))
	for _, file := range files {
		spec := specs[int(file.SpecID())]
		partition := file.Partition()

		result = append(result, IcebergDataFile{
			FilePath:        file.FilePath(),
			FileFormat:      string(file.FileFormat()),
			SpecID:          int64(file.SpecID()),
			RecordCount:     file.Count(),
			FileSizeInBytes: file.FileSizeBytes(),
			Partition:       partitionFields(&spec, partition),
			Values:          browsePartitionValues(&spec, schema, partition),
		})
	}

	return result, nil
}

func partitionSpecsByID(metadata table.Metadata) map[int]iceberg.PartitionSpec {
	specs := make(map[int]iceberg.PartitionSpec)
	for _, spec := range metadata.PartitionSpecs() {
		specs[spec.ID()] = spec
	}

	return specs
}

// liveDataFiles reads the live entries of the current snapshot's data manifests.
// Unlike a scan plan, this keeps files without records and doesn't need to
// resolve delete files, which iceberg-go can't do for equality deletes.
func (c *IcebergClient) liveDataFiles(ctx context.Context, tbl *table.Table) ([]iceberg.DataFile, error) {
	var err error
	var fs icebergio.IO
	var manifests []iceberg.ManifestFile

	snapshot := tbl.CurrentSnapshot()
	if snapshot == nil {
		return []iceberg.DataFile{}, nil
	}

	ctx = utils.WithAwsConfig(ctx, &c.awsCfg)
	if fs, err = tbl.FS(ctx); err != nil {
		return nil, fmt.Errorf("could not open table file io: %w", err)
	}

	if manifests, err = snapshot.Manifests(fs); err != nil {
		return nil, fmt.Errorf("could not read manifest list: %w", err)
	}

	perManifest := make([][]iceberg.DataFile, len(manifests))
	group, _ := errgroup.WithContext(ctx)
	group.SetLimit(manifestReadConcurrency)

	for i, manifest := range manifests {
		if manifest.ManifestContent() != iceberg.ManifestContentData {
			continue
		}

		group.Go(func() error {
			var err error

			if perManifest[i], err = readManifestDataFiles(fs, manifest); err != nil {
				return fmt.Errorf("could not read manifest %s: %w", manifest.FilePath(), err)
			}

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	return slices.Concat(perManifest...), nil
}

// readManifestDataFiles reads a manifest's live entries and decodes their
// partitions. A panic in iceberg-go while reading or decoding becomes an error
// instead of crashing the process.
func readManifestDataFiles(fs icebergio.IO, manifest iceberg.ManifestFile) (files []iceberg.DataFile, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			files = nil
			err = fmt.Errorf("could not decode manifest entries: %v", recovered)
		}
	}()

	for entry, entryErr := range manifest.Entries(fs, true) {
		if entryErr != nil {
			return nil, entryErr
		}

		file := entry.DataFile()
		file.Partition()
		files = append(files, file)
	}

	// A truncated manifest could read as a short one, so check the live entry
	// count, which v2 manifest lists always record.
	live := int(manifest.AddedDataFiles() + manifest.ExistingDataFiles())
	if manifest.Version() >= 2 && len(files) != live {
		return nil, fmt.Errorf("read %d live entries, but the manifest list records %d", len(files), live)
	}

	return files, nil
}

// partitionFields formats a file's partition tuple in spec order. Time
// transforms render as yyyy-mm-dd, the form the browse selection uses.
func partitionFields(spec *iceberg.PartitionSpec, partition map[int]any) []IcebergPartitionField {
	fields := make([]IcebergPartitionField, 0, spec.NumFields())

	for _, pf := range spec.Fields() {
		fields = append(fields, IcebergPartitionField{
			Name:  pf.Name,
			Value: partitionValueText(pf.Transform, partition[pf.FieldID]),
		})
	}

	return fields
}

func partitionValueText(transform iceberg.Transform, value any) string {
	if value == nil {
		return "null"
	}

	if date, ok := partitionDate(transform, value); ok {
		return date.Format(time.DateOnly)
	}

	if v, ok := value.([]byte); ok {
		return string(v)
	}

	return fmt.Sprintf("%v", value)
}

// partitionDate converts a date value or a day, month or year transform result
// to the first day it covers. Manifests store transform results as integer
// offsets from 1970-01-01, which readers may decode as iceberg.Date or int.
func partitionDate(transform iceberg.Transform, value any) (time.Time, bool) {
	if date, ok := value.(iceberg.Date); ok {
		return date.ToTime(), true
	}

	var offset int
	switch v := value.(type) {
	case int:
		offset = v
	case int32:
		offset = int(v)
	case int64:
		offset = int(v)
	default:
		return time.Time{}, false
	}

	switch transform.(type) {
	case iceberg.DayTransform:
		return time.Unix(0, 0).UTC().AddDate(0, 0, offset), true
	case iceberg.MonthTransform:
		return time.Date(1970, time.Month(1+offset), 1, 0, 0, 0, 0, time.UTC), true
	case iceberg.YearTransform:
		return time.Date(1970+offset, time.January, 1, 0, 0, 0, 0, time.UTC), true
	default:
		return time.Time{}, false
	}
}

// ListPartitions returns partition stats with browse-compatible keys
// that match the TableDescription.Partitions names (year, month, day for time transforms,
// or column name for identity transforms).
func (c *IcebergClient) ListPartitions(ctx context.Context, database string, logicalName string) ([]IcebergPartitionStats, error) {
	var err error
	var tbl *table.Table
	var files []iceberg.DataFile

	if tbl, err = c.LoadTable(ctx, database, logicalName); err != nil {
		return nil, fmt.Errorf("could not load table: %w", err)
	}

	currentSnapshot := tbl.CurrentSnapshot()
	if currentSnapshot == nil {
		return []IcebergPartitionStats{}, nil
	}

	specs := partitionSpecsByID(tbl.Metadata())
	schema := tbl.Metadata().CurrentSchema()

	partitionMap := make(map[string]*IcebergPartitionStats)

	if files, err = c.liveDataFiles(ctx, tbl); err != nil {
		return nil, err
	}

	for _, file := range files {
		partition := file.Partition()
		partitionKey := fmt.Sprintf("%d:%s", file.SpecID(), c.partitionKeyString(partition))

		if _, exists := partitionMap[partitionKey]; !exists {
			spec := specs[int(file.SpecID())]

			partitionMap[partitionKey] = &IcebergPartitionStats{
				Partition:      browsePartitionValues(&spec, schema, partition),
				RawPartition:   partition,
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

// browsePartitionValues converts a file's partition, by its own spec, into the
// keys TableDescription.Partitions names: year, month and day for time
// transforms, the source column for identity, the field name otherwise. Refresh
// stores these values and file browse matches against them, so both agree.
// Dates render as yyyy-mm-dd and null stays nil, but a null never replaces a
// value another field set for the same key.
func browsePartitionValues(spec *iceberg.PartitionSpec, schema *iceberg.Schema, partition map[int]any) PartitionValues {
	values := PartitionValues{}
	set := func(key string, value any) {
		if _, exists := values[key]; exists && value == nil {
			return
		}

		values[key] = value
	}

	for _, pf := range spec.Fields() {
		value := partition[pf.FieldID]
		keys := timeTransformKeys[pf.Transform.String()]

		switch {
		case keys != nil:
			date, isDate := partitionDate(pf.Transform, value)
			for _, key := range keys {
				if isDate {
					set(key, date.Format(timeKeyLayouts[key]))
				} else {
					set(key, nil)
				}
			}
		case pf.Transform.String() == "identity":
			name, ok := findSourceColumnName(schema, pf.SourceID())
			if !ok {
				name = pf.Name
			}

			set(name, browseValue(value))
		default:
			set(pf.Name, browseValue(value))
		}
	}

	return values
}

var (
	timeTransformKeys = map[string][]string{
		transformDay:   {"year", "month", "day"},
		transformMonth: {"year", "month"},
		transformYear:  {"year"},
	}
	timeKeyLayouts = map[string]string{"year": "2006", "month": "01", "day": "02"}
)

// identityDate is an identity partition value of a date column, as yyyy-mm-dd.
type identityDate string

// browseValue keeps a partition value as stored JSON, with dates as yyyy-mm-dd.
func browseValue(value any) any {
	switch v := value.(type) {
	case iceberg.Date:
		return identityDate(v.ToTime().Format(time.DateOnly))
	case []byte:
		return string(v)
	default:
		return value
	}
}

func findSourceColumnName(schema *iceberg.Schema, sourceID int) (string, bool) {
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
	var err error
	var tbl *table.Table
	var columns db.JSON[TableColumns, db.NonNullable]
	var partitions db.JSON[[]TablePartition, db.NonNullable]

	if tbl, err = c.LoadTable(ctx, database, logicalName); err != nil {
		return nil, fmt.Errorf("could not load table: %w", err)
	}

	metadata := tbl.Metadata()

	if columns, err = c.extractColumns(metadata.CurrentSchema()); err != nil {
		return nil, fmt.Errorf("could not extract columns: %w", err)
	}

	if partitions, err = c.extractPartitions(metadata); err != nil {
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
	var err error
	var namespaces []table.Identifier

	ctx = utils.WithAwsConfig(ctx, &c.awsCfg)

	if namespaces, err = c.catalog.ListNamespaces(ctx, nil); err != nil {
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
	// The default spec id is an id, not an index into the spec list.
	spec, ok := partitionSpecsByID(metadata)[metadata.DefaultPartitionSpec()]
	if !ok {
		return db.NewJSON([]TablePartition{}, db.NonNullable{}), nil
	}

	partitions := make([]TablePartition, 0)
	fields := spec.Fields()
	schema := metadata.CurrentSchema()

	for _, pf := range fields {
		sourceColumnName, found := findSourceColumnName(schema, pf.SourceID())
		if !found {
			return db.NewJSON(partitions, db.NonNullable{}), fmt.Errorf("could not find source field with id %d for partition field %s", pf.SourceID(), pf.Name)
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
