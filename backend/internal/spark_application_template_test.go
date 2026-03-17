package internal

import (
	"strings"
	"testing"
	"time"
)

func TestLoadSparkApplicationTemplate(t *testing.T) {
	manifest, err := LoadSparkApplicationTemplate()
	if err != nil {
		t.Fatalf("expected template to load, got error: %s", err)
	}

	if manifest.Metadata.Name != "maintenance-task" {
		t.Fatalf("expected generic manifest name, got %q", manifest.Metadata.Name)
	}

	if !strings.HasSuffix(manifest.Spec.PyFiles, "/rewrite_data_files.py") {
		t.Fatalf("expected default pyFiles to point at rewrite_data_files.py, got %q", manifest.Spec.PyFiles)
	}
}

func TestSparkApplicationManifestSetPyFileName(t *testing.T) {
	manifest, err := LoadSparkApplicationTemplate()
	if err != nil {
		t.Fatalf("expected template to load, got error: %s", err)
	}

	if err = manifest.SetPyFileName("expire_snapshots.py"); err != nil {
		t.Fatalf("expected pyFiles update to succeed, got error: %s", err)
	}

	if !strings.HasSuffix(manifest.Spec.PyFiles, "/expire_snapshots.py") {
		t.Fatalf("expected pyFiles to end with expire_snapshots.py, got %q", manifest.Spec.PyFiles)
	}
}

func TestSparkApplicationManifestToCreateUnstructuredOmitsStatus(t *testing.T) {
	manifest, err := LoadSparkApplicationTemplate()
	if err != nil {
		t.Fatalf("expected template to load, got error: %s", err)
	}

	manifest.Metadata.Name = "rewrite-data-files-test"
	manifest.Status = SparkApplicationStatus{
		ApplicationState: SparkApplicationState{State: "SUCCEEDED"},
		CurrentState:     SparkApplicationState{CurrentStateSummary: "ResourceReleased"},
		ErrorMessage:     "should not be sent on create",
	}

	resource, err := manifest.ToCreateUnstructured()
	if err != nil {
		t.Fatalf("expected create payload conversion to succeed, got error: %s", err)
	}

	if got := resource.GetAPIVersion(); got != manifest.APIVersion {
		t.Fatalf("expected apiVersion %q, got %q", manifest.APIVersion, got)
	}

	if got := resource.GetKind(); got != manifest.Kind {
		t.Fatalf("expected kind %q, got %q", manifest.Kind, got)
	}

	if got := resource.GetName(); got != manifest.Metadata.Name {
		t.Fatalf("expected metadata.name %q, got %q", manifest.Metadata.Name, got)
	}

	if _, ok := resource.Object["spec"]; !ok {
		t.Fatal("expected create payload to include spec")
	}

	if _, ok := resource.Object["status"]; ok {
		t.Fatal("expected create payload to omit status")
	}
}

func TestSparkApplicationStatusResolve_CurrentStateSummary(t *testing.T) {
	status := SparkApplicationStatus{
		CurrentState: SparkApplicationState{
			CurrentStateSummary: "RunningHealthy",
			Message:             "Application is running healthy.",
		},
	}

	resolved := status.Resolve()

	if got := resolved.State(); got != "RunningHealthy" {
		t.Fatalf("expected resolved state RunningHealthy, got %q", got)
	}

	if resolved.IsTerminal() {
		t.Fatal("expected running state to be non-terminal")
	}

	if got := resolved.Message; got != "Application is running healthy." {
		t.Fatalf("expected running message, got %q", got)
	}
}

func TestSparkApplicationStatusResolve_TerminatedWithoutReleaseResourcesUsesFailureTransition(t *testing.T) {
	status := SparkApplicationStatus{
		CurrentState: SparkApplicationState{
			CurrentStateSummary: "TerminatedWithoutReleaseResources",
			Message:             "Application is terminated without releasing resources as configured.",
		},
		StateTransitionHistory: map[string]SparkApplicationState{
			"0": {CurrentStateSummary: "Submitted"},
			"1": {CurrentStateSummary: "RunningHealthy"},
			"2": {
				CurrentStateSummary: "Failed",
				Message:             "Driver has one or more failed critical container(s), refer last observed status for details.",
			},
			"3": {
				CurrentStateSummary: "TerminatedWithoutReleaseResources",
				Message:             "Application is terminated without releasing resources as configured.",
			},
		},
	}

	resolved := status.Resolve()

	if got := resolved.State(); got != "Failed" {
		t.Fatalf("expected resolved state Failed, got %q", got)
	}

	if !resolved.IsTerminal() {
		t.Fatal("expected resolved state to be terminal")
	}

	if resolved.IsSuccess() {
		t.Fatal("expected resolved state to be unsuccessful")
	}

	if got := resolved.Message; got != "Driver has one or more failed critical container(s), refer last observed status for details." {
		t.Fatalf("unexpected resolved message %q", got)
	}

	if got := resolved.CurrentState; got != "TerminatedWithoutReleaseResources" {
		t.Fatalf("expected current state to preserve final wrapper state, got %q", got)
	}
}

func TestSparkApplicationStatusResolve_LegacyApplicationState(t *testing.T) {
	status := SparkApplicationStatus{
		ApplicationState: SparkApplicationState{State: "SUCCEEDED"},
		ErrorMessage:     "legacy message",
	}

	resolved := status.Resolve()

	if got := resolved.State(); got != "SUCCEEDED" {
		t.Fatalf("expected legacy resolved state SUCCEEDED, got %q", got)
	}

	if !resolved.IsTerminal() {
		t.Fatal("expected succeeded state to be terminal")
	}

	if !resolved.IsSuccess() {
		t.Fatal("expected succeeded state to be successful")
	}

	if got := resolved.Message; got != "legacy message" {
		t.Fatalf("expected legacy error message fallback, got %q", got)
	}
}

func TestSparkApplicationStatusResolve_ResourceReleasedUsesSuccessTransition(t *testing.T) {
	status := SparkApplicationStatus{
		CurrentState: SparkApplicationState{
			CurrentStateSummary: "ResourceReleased",
		},
		StateTransitionHistory: map[string]SparkApplicationState{
			"0": {CurrentStateSummary: "Submitted"},
			"1": {CurrentStateSummary: "RunningHealthy"},
			"2": {
				CurrentStateSummary: "Succeeded",
				Message:             "Driver has critical container(s) exited with 0.",
			},
			"3": {CurrentStateSummary: "ResourceReleased"},
		},
	}

	resolved := status.Resolve()

	if got := resolved.State(); got != "Succeeded" {
		t.Fatalf("expected resolved state Succeeded, got %q", got)
	}

	if !resolved.IsTerminal() {
		t.Fatal("expected resolved state to be terminal")
	}

	if !resolved.IsSuccess() {
		t.Fatal("expected resolved state to be successful")
	}

	if got := resolved.Message; got != "Driver has critical container(s) exited with 0." {
		t.Fatalf("unexpected resolved message %q", got)
	}

	if got := resolved.CurrentState; got != "ResourceReleased" {
		t.Fatalf("expected current state to preserve final wrapper state, got %q", got)
	}
}

func TestSparkApplicationStatusTransitionResults(t *testing.T) {
	t1 := DateTime{Time: time.Date(2026, 3, 12, 15, 3, 35, 940781810, time.UTC)}
	t2 := DateTime{Time: time.Date(2026, 3, 12, 15, 4, 32, 55261162, time.UTC)}
	t3 := DateTime{Time: time.Date(2026, 3, 12, 15, 4, 53, 343464316, time.UTC)}

	status := SparkApplicationStatus{
		StateTransitionHistory: map[string]SparkApplicationState{
			"2": {
				CurrentStateSummary: "Succeeded",
				Message:             "Driver has critical container(s) exited with 0.",
				LastTransitionTime:  t2,
			},
			"0": {
				CurrentStateSummary: "Submitted",
				Message:             "Spark application has been created on Kubernetes Cluster.",
				LastTransitionTime:  t1,
			},
			"3": {
				CurrentStateSummary: "ResourceReleased",
				LastTransitionTime:  t3,
			},
		},
	}

	transitions := status.TransitionResults()

	if len(transitions) != 3 {
		t.Fatalf("expected 3 transitions, got %d", len(transitions))
	}

	if transitions[0].State != "Submitted" || !transitions[0].Timestamp.Equal(t1.Time) {
		t.Fatalf("unexpected first transition: %#v", transitions[0])
	}

	if transitions[1].State != "Succeeded" || transitions[1].Message != "Driver has critical container(s) exited with 0." || !transitions[1].Timestamp.Equal(t2.Time) {
		t.Fatalf("unexpected second transition: %#v", transitions[1])
	}

	if transitions[2].State != "ResourceReleased" || !transitions[2].Timestamp.Equal(t3.Time) {
		t.Fatalf("unexpected third transition: %#v", transitions[2])
	}
}
