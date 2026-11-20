package internal

import (
	"testing"

	iceberg "github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

var browseTestFields = []TablePartition{
	{Name: "year", RawFieldName: "createdAt_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "createdAt", Type: transformDay}},
	{Name: "month", RawFieldName: "createdAt_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "createdAt", Type: transformDay}},
	{Name: "day", RawFieldName: "createdAt_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "createdAt", Type: transformDay}},
	{Name: "businessUnitId", RawFieldName: "businessUnitId"},
}

func TestValidateBrowseFiltersRequiresACompleteKnownSelection(t *testing.T) {
	require.NoError(t, validateBrowseFilters(browseTestFields, map[string]string{"year": "2026", "month": "03", "day": "25", "businessUnitId": "1"}))
	require.EqualError(t, validateBrowseFilters(browseTestFields, map[string]string{"year": "2026", "month": "03"}), "listing data files requires a complete partition selection")
	require.EqualError(t, validateBrowseFilters(browseTestFields, map[string]string{"unknown": "value"}), "unknown partition key \"unknown\"")
}

func TestFilterBrowseFilesKeepsSelectedPartitionLargestFirst(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "createdAt", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 2, Name: "businessUnitId", Type: iceberg.PrimitiveTypes.Int32},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "createdAt_day", Transform: iceberg.DayTransform{}},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "businessUnitId", Transform: iceberg.IdentityTransform{}},
	)
	day := iceberg.Date(20537) // 2026-03-25
	file := func(path string, size int64, date any, businessUnitID int32) IcebergDataFile {
		partition := map[int]any{1000: date, 1001: businessUnitID}

		return IcebergDataFile{
			FilePath:        path,
			FileFormat:      "PARQUET",
			FileSizeInBytes: size,
			Partition:       partitionFields(&spec, partition),
			Values:          browsePartitionValues(&spec, schema, partition),
		}
	}

	items := filterBrowseFiles([]IcebergDataFile{
		file("s3://b/small.parquet", 10, day, 1),
		file("s3://b/other-day.parquet", 50, day+1, 1),
		file("s3://b/other-unit.parquet", 50, day, 2),
		file("s3://b/null-day.parquet", 50, nil, 1),
		file("s3://b/large.parquet", 30, day, 1),
	}, map[string]string{"year": "2026", "month": "03", "day": "25", "businessUnitId": "1"})

	require.Equal(t, []DataFileItem{
		{FilePath: "s3://b/large.parquet", FileFormat: "PARQUET", Partition: "{createdAt_day=2026-03-25, businessUnitId=1}", FileSizeInBytes: 30},
		{FilePath: "s3://b/small.parquet", FileFormat: "PARQUET", Partition: "{createdAt_day=2026-03-25, businessUnitId=1}", FileSizeInBytes: 10},
	}, items)
}

func TestPartitionValueTextRendersTimeTransformsAsSelectionDates(t *testing.T) {
	require.Equal(t, "2026-03-25", partitionValueText(iceberg.DayTransform{}, iceberg.Date(20537)))
	require.Equal(t, "2026-03-25", partitionValueText(iceberg.DayTransform{}, int32(20537)))
	require.Equal(t, "2026-03-25", partitionValueText(iceberg.DayTransform{}, 20537))
	require.Equal(t, "2026-03-01", partitionValueText(iceberg.MonthTransform{}, int32(674)))
	require.Equal(t, "2026-03-01", partitionValueText(iceberg.MonthTransform{}, 674))
	require.Equal(t, "2026-01-01", partitionValueText(iceberg.YearTransform{}, 56))
}

func TestMatchesBrowseFiltersKeepsNullDistinctFromLiteralNull(t *testing.T) {
	filters := map[string]string{"country": "null"}

	require.False(t, matchesBrowseFilters(PartitionValues{"country": nil}, filters))
	require.False(t, matchesBrowseFilters(PartitionValues{}, filters))
	require.True(t, matchesBrowseFilters(PartitionValues{"country": "null"}, filters))
}

func TestMatchesBrowseFiltersAcceptsIdentityDatesStoredAsDays(t *testing.T) {
	values := PartitionValues{"eventDate": identityDate("2026-03-25")}

	require.True(t, matchesBrowseFilters(values, map[string]string{"eventDate": "2026-03-25"}))
	require.True(t, matchesBrowseFilters(values, map[string]string{"eventDate": "20537"}))
	require.False(t, matchesBrowseFilters(values, map[string]string{"eventDate": "20538"}))

	// Only identity dates have the old form; a string column keeps its text.
	require.False(t, matchesBrowseFilters(PartitionValues{"label": "2026-03-25"}, map[string]string{"label": "20537"}))
}
