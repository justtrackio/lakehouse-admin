package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSparkApplicationManifestMergesDriverPodSettingsOnly(t *testing.T) {
	manifest, err := LoadSparkApplicationTemplate()
	require.NoError(t, err)

	existingExecutorAnnotations := manifest.Spec.ExecutorSpec.PodTemplateSpec.Metadata.Annotations
	existingExecutorNodeSelector := manifest.Spec.ExecutorSpec.PodTemplateSpec.Spec.NodeSelector
	existingExecutorTolerations := manifest.Spec.ExecutorSpec.PodTemplateSpec.Spec.Tolerations

	manifest.Spec.DriverSpec.PodTemplateSpec.Metadata.Annotations = map[string]string{
		"existing": "driver",
		"shared":   "template",
	}
	manifest.Spec.DriverSpec.PodTemplateSpec.Spec.NodeSelector = map[string]string{
		"existing": "driver",
		"shared":   "template",
	}
	manifest.Spec.DriverSpec.PodTemplateSpec.Spec.Tolerations = []SparkApplicationToleration{{
		Key:      "existing",
		Operator: "Exists",
	}}

	manifest.MergeDriverPodAnnotations(map[string]string{
		"shared": "config",
		"team":   "lakehouse",
	})
	manifest.MergeDriverNodeSelector(map[string]string{
		"shared":   "config",
		"nodepool": "spark",
	})
	manifest.AppendDriverTolerations([]SparkApplicationToleration{{
		Key:      "dedicated",
		Operator: "Equal",
		Value:    "analytics",
	}})

	require.Equal(t, map[string]string{
		"existing": "driver",
		"shared":   "config",
		"team":     "lakehouse",
	}, manifest.Spec.DriverSpec.PodTemplateSpec.Metadata.Annotations)
	require.Equal(t, map[string]string{
		"existing": "driver",
		"shared":   "config",
		"nodepool": "spark",
	}, manifest.Spec.DriverSpec.PodTemplateSpec.Spec.NodeSelector)
	require.Equal(t, []SparkApplicationToleration{
		{Key: "existing", Operator: "Exists"},
		{Key: "dedicated", Operator: "Equal", Value: "analytics"},
	}, manifest.Spec.DriverSpec.PodTemplateSpec.Spec.Tolerations)

	require.Equal(t, existingExecutorAnnotations, manifest.Spec.ExecutorSpec.PodTemplateSpec.Metadata.Annotations)
	require.Equal(t, existingExecutorNodeSelector, manifest.Spec.ExecutorSpec.PodTemplateSpec.Spec.NodeSelector)
	require.Equal(t, existingExecutorTolerations, manifest.Spec.ExecutorSpec.PodTemplateSpec.Spec.Tolerations)
}
