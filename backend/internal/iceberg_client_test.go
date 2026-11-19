package internal

import (
	"bytes"
	"iter"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestBrowsePartitionValuesUsesNestedIdentityColumnName(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "goal",
			Type: &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 2, Name: "conversionHappenedAt", Type: iceberg.PrimitiveTypes.Date}}},
		},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Name: "goal_conversion_happened_at", Transform: iceberg.IdentityTransform{}},
	)

	// Identity dates take the same yyyy-mm-dd form as the file browse matches.
	partition := browsePartitionValues(&spec, schema, map[int]any{1000: iceberg.Date(20164)})

	require.Equal(t, PartitionValues{"goal.conversionHappenedAt": "2025-03-17"}, partition)
}

func TestBrowsePartitionValuesKeepsHiddenTimePartitionsForNestedColumn(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "goal",
			Type: &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 2, Name: "conversionHappenedAt", Type: iceberg.PrimitiveTypes.Date}}},
		},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Name: "goal_conversion_happened_at_day", Transform: iceberg.DayTransform{}},
	)

	for _, day := range []any{iceberg.Date(20164), int32(20164), 20164} {
		partition := browsePartitionValues(&spec, schema, map[int]any{1000: day})

		require.Equal(t, PartitionValues{"year": "2025", "month": "03", "day": "17"}, partition)
	}
}

func TestBrowsePartitionValuesKeepsAYearNextToANullDay(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "createdAt", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 2, Name: "updatedAt", Type: iceberg.PrimitiveTypes.TimestampTz},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "createdAt_day", Transform: iceberg.DayTransform{}},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "updatedAt_year", Transform: iceberg.YearTransform{}},
	)

	partition := browsePartitionValues(&spec, schema, map[int]any{1000: nil, 1001: int32(56)})

	require.Equal(t, PartitionValues{"year": "2026", "month": nil, "day": nil}, partition)
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
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Name: "goal_conversion_happened_at_day", Transform: iceberg.DayTransform{}},
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
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Name: "goal_conversion_happened_at", Transform: iceberg.IdentityTransform{}},
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

// testTableMetadata implements the table.Metadata methods extractPartitions uses.
type testTableMetadata struct {
	table.Metadata
	schema *iceberg.Schema
	specs  []iceberg.PartitionSpec
}

func (m *testTableMetadata) CurrentSchema() *iceberg.Schema          { return m.schema }
func (m *testTableMetadata) PartitionSpecs() []iceberg.PartitionSpec { return m.specs }
func (m *testTableMetadata) DefaultPartitionSpec() int               { return 0 }

// inMemoryManifest reads its entries from data instead of the table's file io.
type inMemoryManifest struct {
	iceberg.ManifestFile
	data []byte
}

func (m inMemoryManifest) Entries(_ icebergio.IO, discardDeleted bool) iter.Seq2[iceberg.ManifestEntry, error] {
	entries, err := iceberg.ReadManifest(m.ManifestFile, bytes.NewReader(m.data), discardDeleted)

	return func(yield func(iceberg.ManifestEntry, error) bool) {
		if err != nil {
			yield(nil, err)

			return
		}

		for _, entry := range entries {
			if !yield(entry, nil) {
				return
			}
		}
	}
}

type panickingManifest struct{ iceberg.ManifestFile }

func (panickingManifest) Entries(icebergio.IO, bool) iter.Seq2[iceberg.ManifestEntry, error] {
	panic("interface conversion: interface {} is nil, not int32")
}

func TestReadManifestDataFilesTurnsPanicIntoError(t *testing.T) {
	files, err := readManifestDataFiles(nil, panickingManifest{})

	require.Nil(t, files)
	require.ErrorContains(t, err, "could not decode manifest entries")
}

func TestReadManifestDataFilesKeepsFilesNextToANullDatePartition(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "createdAt", Type: iceberg.PrimitiveTypes.TimestampTz})
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "createdAt_day", Transform: iceberg.DayTransform{}},
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
		entry("s3://b/day.parquet", iceberg.Date(20537)),
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), `"logicalType":"date"`, "the manifest must carry the Avro date type that v0.4.0 failed on")

	files, err := readManifestDataFiles(nil, inMemoryManifest{ManifestFile: manifest, data: buf.Bytes()})
	require.NoError(t, err)
	require.Equal(t, map[int]any{1000: nil}, files[0].Partition())
	require.Equal(t, map[int]any{1000: iceberg.Date(20537)}, files[1].Partition())

	browse := make([]IcebergDataFile, len(files))
	for i, file := range files {
		browse[i] = IcebergDataFile{FilePath: file.FilePath(), Values: browsePartitionValues(&spec, schema, file.Partition())}
	}

	items := filterBrowseFiles(browse, map[string]string{"year": "2026", "month": "03", "day": "25"})
	require.Len(t, items, 1)
	require.Equal(t, "s3://b/day.parquet", items[0].FilePath)
}
