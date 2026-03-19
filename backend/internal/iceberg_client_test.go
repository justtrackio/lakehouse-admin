package internal

import (
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

	partition := client.normalizePartitionForBrowse(map[int]any{1000: iceberg.Date(20164)}, &spec, schema)

	require.Equal(t, PartitionValues{"year": "2025", "month": "03", "day": "17"}, PartitionValues(partition))
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
		{Name: "year", IsHidden: true, Hidden: TablePartitionHidden{Column: "goal.conversionHappenedAt", Type: transformDay}},
		{Name: "month", IsHidden: true, Hidden: TablePartitionHidden{Column: "goal.conversionHappenedAt", Type: transformDay}},
		{Name: "day", IsHidden: true, Hidden: TablePartitionHidden{Column: "goal.conversionHappenedAt", Type: transformDay}},
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
	require.Equal(t, []TablePartition{{Name: "goal.conversionHappenedAt", IsHidden: false, Hidden: TablePartitionHidden{}}}, partitions.Get())
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
