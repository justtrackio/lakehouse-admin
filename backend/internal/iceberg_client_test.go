package internal

import (
	"bytes"
	"encoding/binary"
	"iter"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNormalizePartitionForBrowseUsesNestedIdentityColumnName(t *testing.T) {
	client := &IcebergClient{}
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "goal",
			Type: &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 2, Name: "conversionHappenedAt", Type: iceberg.PrimitiveTypes.Date}}},
		},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 2, FieldID: 1000, Name: "goal_conversion_happened_at", Transform: iceberg.IdentityTransform{}},
	)

	partition := client.normalizePartitionForBrowse(map[int]any{1000: "2026-03-17"}, &spec, schema)

	require.Equal(t, PartitionValues{"goal.conversionHappenedAt": "2026-03-17"}, PartitionValues(partition))
}

func TestNormalizePartitionForBrowseKeepsHiddenTimePartitionsForNestedColumn(t *testing.T) {
	client := &IcebergClient{}
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "goal",
			Type: &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 2, Name: "conversionHappenedAt", Type: iceberg.PrimitiveTypes.Date}}},
		},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 2, FieldID: 1000, Name: "goal_conversion_happened_at_day", Transform: iceberg.DayTransform{}},
	)

	for _, day := range []any{iceberg.Date(20164), int32(20164), 20164} {
		partition := client.normalizePartitionForBrowse(map[int]any{1000: day}, &spec, schema)

		require.Equal(t, PartitionValues{"year": "2025", "month": "03", "day": "17"}, PartitionValues(partition))
	}
}

func TestExtractPartitionsUsesNestedColumnName(t *testing.T) {
	client := &IcebergClient{}
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "goal",
			Type: &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 2, Name: "conversionHappenedAt", Type: iceberg.PrimitiveTypes.Date}}},
		},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 2, FieldID: 1000, Name: "goal_conversion_happened_at_day", Transform: iceberg.DayTransform{}},
	)
	metadata := &testTableMetadata{schema: schema, specs: []iceberg.PartitionSpec{spec}}

	partitions, err := client.extractPartitions(metadata)
	require.NoError(t, err)
	require.Equal(t, []TablePartition{
		{Name: "year", RawFieldName: "goal_conversion_happened_at_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "goal.conversionHappenedAt", Type: transformDay}},
		{Name: "month", RawFieldName: "goal_conversion_happened_at_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "goal.conversionHappenedAt", Type: transformDay}},
		{Name: "day", RawFieldName: "goal_conversion_happened_at_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "goal.conversionHappenedAt", Type: transformDay}},
	}, partitions.Get())
}

func TestExtractPartitionsUsesNestedIdentityColumnName(t *testing.T) {
	client := &IcebergClient{}
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "goal",
			Type: &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 2, Name: "conversionHappenedAt", Type: iceberg.PrimitiveTypes.Date}}},
		},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 2, FieldID: 1000, Name: "goal_conversion_happened_at", Transform: iceberg.IdentityTransform{}},
	)
	metadata := &testTableMetadata{schema: schema, specs: []iceberg.PartitionSpec{spec}}

	partitions, err := client.extractPartitions(metadata)
	require.NoError(t, err)
	require.Equal(t, []TablePartition{{Name: "goal.conversionHappenedAt", RawFieldName: "goal_conversion_happened_at", IsHidden: false, Hidden: TablePartitionHidden{}}}, partitions.Get())
}

func TestPartitionJSONPathExprQuotesLiteralKeys(t *testing.T) {
	require.Equal(t, `p.partition->>'$."goal.conversionHappenedAt"'`, partitionJSONPathExpr("goal.conversionHappenedAt", true))
	require.Equal(t, `p.partition->'$."goal.conversionHappenedAt"'`, partitionJSONPathExpr("goal.conversionHappenedAt", false))
}

type testTableMetadata struct {
	schema *iceberg.Schema
	specs  []iceberg.PartitionSpec
}

func (m *testTableMetadata) Version() int                            { return 0 }
func (m *testTableMetadata) TableUUID() uuid.UUID                    { return uuid.Nil }
func (m *testTableMetadata) Location() string                        { return "" }
func (m *testTableMetadata) LastUpdatedMillis() int64                { return 0 }
func (m *testTableMetadata) LastColumnID() int                       { return 0 }
func (m *testTableMetadata) Schemas() []*iceberg.Schema              { return []*iceberg.Schema{m.schema} }
func (m *testTableMetadata) CurrentSchema() *iceberg.Schema          { return m.schema }
func (m *testTableMetadata) PartitionSpecs() []iceberg.PartitionSpec { return m.specs }
func (m *testTableMetadata) PartitionSpec() iceberg.PartitionSpec {
	if len(m.specs) == 0 {
		return iceberg.NewPartitionSpec()
	}

	return m.specs[0]
}
func (m *testTableMetadata) DefaultPartitionSpec() int   { return 0 }
func (m *testTableMetadata) LastPartitionSpecID() *int   { return nil }
func (m *testTableMetadata) Snapshots() []table.Snapshot { return nil }
func (m *testTableMetadata) SnapshotByID(int64) *table.Snapshot {
	return nil
}
func (m *testTableMetadata) SnapshotByName(string) *table.Snapshot { return nil }
func (m *testTableMetadata) CurrentSnapshot() *table.Snapshot      { return nil }
func (m *testTableMetadata) Ref() table.SnapshotRef                { return table.SnapshotRef{} }
func (m *testTableMetadata) Refs() iter.Seq2[string, table.SnapshotRef] {
	return func(func(string, table.SnapshotRef) bool) {}
}

func (m *testTableMetadata) SnapshotLogs() iter.Seq[table.SnapshotLogEntry] {
	return func(func(table.SnapshotLogEntry) bool) {}
}
func (m *testTableMetadata) SortOrder() table.SortOrder { return table.SortOrder{} }
func (m *testTableMetadata) SortOrders() []table.SortOrder {
	return nil
}
func (m *testTableMetadata) DefaultSortOrder() int          { return 0 }
func (m *testTableMetadata) Properties() iceberg.Properties { return nil }
func (m *testTableMetadata) PreviousFiles() iter.Seq[table.MetadataLogEntry] {
	return func(func(table.MetadataLogEntry) bool) {}
}
func (m *testTableMetadata) Equals(table.Metadata) bool { return false }
func (m *testTableMetadata) NameMapping() iceberg.NameMapping {
	return nil
}
func (m *testTableMetadata) LastSequenceNumber() int64 { return 0 }
func (m *testTableMetadata) Statistics() iter.Seq[table.StatisticsFile] {
	return func(func(table.StatisticsFile) bool) {}
}

func (m *testTableMetadata) PartitionStatistics() iter.Seq[table.PartitionStatisticsFile] {
	return func(func(table.PartitionStatisticsFile) bool) {}
}

// panickingPartitionEntry stands in for a decoder that panics on Partition().
type panickingPartitionEntry struct{ iceberg.ManifestEntry }

func (panickingPartitionEntry) DataFile() iceberg.DataFile { return panickingPartitionFile{} }

type panickingPartitionFile struct{ iceberg.DataFile }

func (panickingPartitionFile) SpecID() int32 { return 0 }

func (panickingPartitionFile) Partition() map[int]any {
	panic("interface conversion: interface {} is nil, not int32")
}

func TestDecodeDataFilesTurnsDecoderPanicIntoError(t *testing.T) {
	files, err := decodeDataFiles([]iceberg.ManifestEntry{panickingPartitionEntry{}}, nil)

	require.Nil(t, files)
	require.ErrorContains(t, err, "could not decode manifest entries")
}

func TestDecodeDataFilesKeepsFilesNextToANullDatePartition(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "createdAt", Type: iceberg.PrimitiveTypes.TimestampTz})
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 1, FieldID: 1000, Name: "createdAt_day", Transform: iceberg.DayTransform{}},
	)
	snapshotID := int64(1)
	entry := func(path string, day any) iceberg.ManifestEntry {
		builder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, path, iceberg.ParquetFile, map[int]any{1000: day}, nil, nil, 1, 10)
		require.NoError(t, err)

		return iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, builder.Build())
	}

	var buf bytes.Buffer
	manifest, err := iceberg.WriteManifest("m.avro", &buf, 2, spec, schema, snapshotID, []iceberg.ManifestEntry{
		entry("s3://b/null-day.parquet", nil),
		entry("s3://b/day.parquet", int32(20537)),
	})
	require.NoError(t, err)

	// Spark and Flink write the partition date with its Avro logical type,
	// which iceberg-go's writer leaves out.
	data := withAvroSchemaReplaced(t, buf.Bytes(), `{"name":"createdAt_day","type":["null","int"]`, `{"name":"createdAt_day","type":["null",{"type":"int","logicalType":"date"}]`)

	read := func() []iceberg.ManifestEntry {
		entries, err := iceberg.ReadManifest(manifest, bytes.NewReader(data), true)
		require.NoError(t, err)

		return entries
	}

	// The upstream bug this works around (apache/iceberg-go#1179). Drop the
	// workaround once this no longer panics.
	require.Panics(t, func() { read()[0].DataFile().Partition() })

	files, err := decodeDataFiles(read(), map[int]iceberg.PartitionSpec{spec.ID(): spec})
	require.NoError(t, err)
	require.Equal(t, map[int]any{1000: nil}, files[0].Partition())
	require.Equal(t, map[int]any{1000: iceberg.Date(20537)}, files[1].Partition())

	items := filterBrowseFiles([]IcebergDataFile{
		{FilePath: files[0].FilePath(), Partition: partitionFields(&spec, files[0].Partition())},
		{FilePath: files[1].FilePath(), Partition: partitionFields(&spec, files[1].Partition())},
	}, []browseFileSelection{{RawFieldName: "createdAt_day", Value: "2026-03-25"}})
	require.Len(t, items, 1)
	require.Equal(t, "s3://b/day.parquet", items[0].FilePath)
}

func TestPartitionFieldsKeepsNullDatePartition(t *testing.T) {
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 1, FieldID: 1000, Name: "createdAt_day", Transform: iceberg.DayTransform{}},
	)

	require.Equal(t, []IcebergPartitionField{{Name: "createdAt_day", Value: "null", Null: true}}, partitionFields(&spec, map[int]any{1000: nil}))
}

// withAvroSchemaReplaced edits the writer schema in an Avro container file header.
func withAvroSchemaReplaced(t *testing.T, file []byte, old string, replacement string) []byte {
	key := []byte("avro.schema")
	start := bytes.Index(file, key) + len(key)
	require.Greater(t, start, len(key))

	length, n := binary.Varint(file[start:])
	schema := file[start+n : start+n+int(length)]
	require.Contains(t, string(schema), old)

	edited := bytes.Replace(schema, []byte(old), []byte(replacement), 1)
	out := append([]byte{}, file[:start]...)
	out = binary.AppendVarint(out, int64(len(edited)))
	out = append(out, edited...)

	return append(out, file[start+n+int(length):]...)
}
