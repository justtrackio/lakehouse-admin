package internal

import (
	"testing"

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

func TestBuildBrowseFilesQueryUsesFilesMetadataTable(t *testing.T) {
	service := &ServiceBrowseFiles{}

	query := service.buildBrowseFilesQuery("revenueevent", []browseFileSelection{{RawFieldName: "createdAt_day", Value: "2026-03-25"}})

	require.Contains(t, query, `FROM "lakehouse"."main"."revenueevent$files"`)
	require.Contains(t, query, `WHERE content = 0`)
	require.Contains(t, query, `CAST(partition."createdAt_day" AS VARCHAR) = '2026-03-25'`)
}

func TestBrowseRowValueToStringFormatsPartitionTupleWithFieldNames(t *testing.T) {
	service := &ServiceBrowseFiles{}

	value, err := service.browseRowValueToString([]any{"2024-12-24", "2"}, []string{"createdAt_day", "businessUnitId"})
	require.NoError(t, err)
	require.Equal(t, "{createdAt_day=2024-12-24, businessUnitId=2}", value)
}

func TestBrowseRowValueToStringFormatsPartitionMapInSelectionOrder(t *testing.T) {
	service := &ServiceBrowseFiles{}

	value, err := service.browseRowValueToString(
		map[string]any{"businessUnitId": "2", "createdAt_day": "2024-12-24"},
		[]string{"createdAt_day", "businessUnitId"},
	)
	require.NoError(t, err)
	require.Equal(t, "{createdAt_day=2024-12-24, businessUnitId=2}", value)
}
