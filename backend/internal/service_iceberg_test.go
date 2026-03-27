package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPartitionNeedsOptimizeUsesSmallFileCountAndShare(t *testing.T) {
	service := &ServiceIceberg{
		settings: &IcebergSettings{NeedsOptimizeDelay: 24 * time.Hour},
	}

	oldPartitionDate := time.Now().UTC().AddDate(0, 0, -7)

	testCases := []struct {
		name          string
		fileSizes     []int64
		minCount      int64
		minSharePct   int64
		expectedValue bool
	}{
		{
			name:          "requires more than one small file",
			fileSizes:     []int64{32, 300, 320},
			minCount:      2,
			minSharePct:   25,
			expectedValue: false,
		},
		{
			name:          "ignores a couple of small files among many large ones",
			fileSizes:     []int64{32, 64, 300, 320, 340, 360, 380, 400},
			minCount:      2,
			minSharePct:   30,
			expectedValue: false,
		},
		{
			name:          "optimizes when thresholds are met exactly",
			fileSizes:     []int64{32, 64, 300, 320, 340, 360, 380, 400},
			minCount:      2,
			minSharePct:   25,
			expectedValue: true,
		},
		{
			name:          "optimizes when many files are small",
			fileSizes:     []int64{32, 64, 96, 128, 300, 320},
			minCount:      2,
			minSharePct:   50,
			expectedValue: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			stats := IcebergPartitionStats{
				Partition: partitionValuesForDate(oldPartitionDate),
				Files:     partitionFiles(testCase.fileSizes...),
			}

			needsOptimize, err := service.partitionNeedsOptimize(stats, 256, testCase.minCount, testCase.minSharePct)

			require.NoError(t, err)
			require.Equal(t, testCase.expectedValue, needsOptimize)
		})
	}
}

func TestPartitionNeedsOptimizeRespectsNeedsOptimizeDelay(t *testing.T) {
	service := &ServiceIceberg{
		settings: &IcebergSettings{NeedsOptimizeDelay: 48 * time.Hour},
	}

	stats := IcebergPartitionStats{
		Partition: partitionValuesForDate(time.Now().UTC()),
		Files:     partitionFiles(32, 64, 300, 320),
	}

	needsOptimize, err := service.partitionNeedsOptimize(stats, 256, 2, 25)

	require.NoError(t, err)
	require.False(t, needsOptimize)
}

func TestPartitionNeedsOptimizeWithoutDateSkipsDelayCheck(t *testing.T) {
	service := &ServiceIceberg{
		settings: &IcebergSettings{NeedsOptimizeDelay: 30 * 24 * time.Hour},
	}

	stats := IcebergPartitionStats{
		Partition: PartitionValues{"tenant": "acme"},
		Files:     partitionFiles(32, 64, 300, 320),
	}

	needsOptimize, err := service.partitionNeedsOptimize(stats, 256, 2, 25)

	require.NoError(t, err)
	require.True(t, needsOptimize)
}

func partitionValuesForDate(date time.Time) PartitionValues {
	return PartitionValues{
		"year":  date.Format("2006"),
		"month": date.Format("01"),
		"day":   date.Format("02"),
	}
}

func partitionFiles(fileSizes ...int64) IcebergPartitionStatsFiles {
	files := make(IcebergPartitionStatsFiles, 0, len(fileSizes))
	for _, fileSize := range fileSizes {
		files = append(files, IcebergPartitionFileStats{SizeBytes: fileSize})
	}

	return files
}
