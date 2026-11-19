package internal

import (
	"testing"

	iceberg "github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestResolveBrowseFileSelectionsBuildsHiddenAndIdentityPredicates(t *testing.T) {
	service := &ServiceBrowseFiles{}

	fields := []TablePartition{
		{Name: "year", RawFieldName: "createdAt_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "createdAt", Type: transformDay}},
		{Name: "month", RawFieldName: "createdAt_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "createdAt", Type: transformDay}},
		{Name: "day", RawFieldName: "createdAt_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "createdAt", Type: transformDay}},
		{Name: "businessUnitId", RawFieldName: "businessUnitId", IsHidden: false, Hidden: TablePartitionHidden{}},
	}

	selections, err := service.resolveBrowseFileSelections(fields, map[string]string{
		"year":           "2026",
		"month":          "03",
		"day":            "25",
		"businessUnitId": "1",
	})
	require.NoError(t, err)
	require.Equal(t, []browseFileSelection{
		{RawFieldName: "createdAt_day", Value: "2026-03-25"},
		{RawFieldName: "businessUnitId", Value: "1"},
	}, selections)
}

func TestResolveBrowseFileSelectionsRejectsIncompleteSelection(t *testing.T) {
	service := &ServiceBrowseFiles{}

	fields := []TablePartition{
		{Name: "year", RawFieldName: "createdAt_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "createdAt", Type: transformDay}},
		{Name: "month", RawFieldName: "createdAt_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "createdAt", Type: transformDay}},
		{Name: "day", RawFieldName: "createdAt_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "createdAt", Type: transformDay}},
	}

	_, err := service.resolveBrowseFileSelections(fields, map[string]string{
		"year":  "2026",
		"month": "03",
	})
	require.EqualError(t, err, "listing data files requires a complete partition selection")
}

func TestResolveBrowseFileSelectionsRejectsUnknownKeys(t *testing.T) {
	service := &ServiceBrowseFiles{}

	fields := []TablePartition{{Name: "businessUnitId", RawFieldName: "businessUnitId"}}

	_, err := service.resolveBrowseFileSelections(fields, map[string]string{
		"businessUnitId": "1",
		"unknown":        "value",
	})
	require.EqualError(t, err, "unknown partition key \"unknown\"")
}

func TestResolveBrowseFileSelectionsRequiresRawFieldName(t *testing.T) {
	service := &ServiceBrowseFiles{}

	fields := []TablePartition{{Name: "businessUnitId"}}

	_, err := service.resolveBrowseFileSelections(fields, map[string]string{"businessUnitId": "1"})
	require.EqualError(t, err, "partition \"businessUnitId\" is missing raw field metadata")
}

func TestFilterBrowseFilesKeepsSelectedPartitionLargestFirst(t *testing.T) {
	service := &ServiceBrowseFiles{}
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 1, FieldID: 1000, Name: "createdAt_day", Transform: iceberg.DayTransform{}},
		iceberg.PartitionField{SourceID: 2, FieldID: 1001, Name: "businessUnitId", Transform: iceberg.IdentityTransform{}},
	)
	day := iceberg.Date(20537) // 2026-03-25
	file := func(path string, size int64, date iceberg.Date, businessUnitID int32) IcebergDataFile {
		return IcebergDataFile{
			FilePath:        path,
			FileFormat:      "PARQUET",
			FileSizeInBytes: size,
			Partition:       partitionFields(&spec, map[int]any{1000: date, 1001: businessUnitID}),
		}
	}

	selections, err := service.resolveBrowseFileSelections([]TablePartition{
		{Name: "year", RawFieldName: "createdAt_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "createdAt", Type: transformDay}},
		{Name: "month", RawFieldName: "createdAt_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "createdAt", Type: transformDay}},
		{Name: "day", RawFieldName: "createdAt_day", IsHidden: true, Hidden: TablePartitionHidden{Column: "createdAt", Type: transformDay}},
		{Name: "businessUnitId", RawFieldName: "businessUnitId"},
	}, map[string]string{"year": "2026", "month": "03", "day": "25", "businessUnitId": "1"})
	require.NoError(t, err)

	items := filterBrowseFiles([]IcebergDataFile{
		file("s3://b/small.parquet", 10, day, 1),
		file("s3://b/other-day.parquet", 50, day+1, 1),
		file("s3://b/other-unit.parquet", 50, day, 2),
		file("s3://b/large.parquet", 30, day, 1),
	}, selections)

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

func TestMatchesBrowseSelectionsKeepsNullDistinctFromLiteralNull(t *testing.T) {
	selection := []browseFileSelection{{RawFieldName: "country", Value: "null"}}

	require.False(t, matchesBrowseSelections([]IcebergPartitionField{{Name: "country", Value: "null", Null: true}}, selection))
	require.True(t, matchesBrowseSelections([]IcebergPartitionField{{Name: "country", Value: "null"}}, selection))
}
