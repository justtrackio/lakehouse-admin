package internal

import "testing"

func TestShouldHandleSparkApplicationUpdate_NewTerminalState(t *testing.T) {
	oldStatus := SparkApplicationStatus{
		CurrentState: SparkApplicationState{CurrentStateSummary: "RunningHealthy"},
	}
	newStatus := SparkApplicationStatus{
		CurrentState: SparkApplicationState{CurrentStateSummary: "Succeeded"},
	}

	if !shouldHandleSparkApplicationUpdate(oldStatus, newStatus) {
		t.Fatal("expected update when application newly becomes terminal")
	}
}

func TestShouldHandleSparkApplicationUpdate_DuplicateTerminalState(t *testing.T) {
	oldStatus := SparkApplicationStatus{
		CurrentState: SparkApplicationState{CurrentStateSummary: "ResourceReleased"},
		StateTransitionHistory: map[string]SparkApplicationState{
			"0": {CurrentStateSummary: "Submitted"},
			"1": {CurrentStateSummary: "Succeeded"},
		},
	}
	newStatus := SparkApplicationStatus{
		CurrentState: SparkApplicationState{CurrentStateSummary: "ResourceReleased"},
		StateTransitionHistory: map[string]SparkApplicationState{
			"0": {CurrentStateSummary: "Submitted"},
			"1": {CurrentStateSummary: "Succeeded"},
			"2": {CurrentStateSummary: "ResourceReleased"},
		},
	}

	if shouldHandleSparkApplicationUpdate(oldStatus, newStatus) {
		t.Fatal("expected duplicate terminal update to be ignored")
	}
}

func TestShouldHandleSparkApplicationUpdate_TerminalOutcomeChanges(t *testing.T) {
	oldStatus := SparkApplicationStatus{
		CurrentState: SparkApplicationState{CurrentStateSummary: "Failed"},
	}
	newStatus := SparkApplicationStatus{
		CurrentState: SparkApplicationState{CurrentStateSummary: "Succeeded"},
	}

	if !shouldHandleSparkApplicationUpdate(oldStatus, newStatus) {
		t.Fatal("expected terminal update when resolved outcome changes")
	}
}

func TestShouldHandleSparkApplicationUpdate_NonTerminalUpdate(t *testing.T) {
	oldStatus := SparkApplicationStatus{
		CurrentState: SparkApplicationState{CurrentStateSummary: "Submitted"},
	}
	newStatus := SparkApplicationStatus{
		CurrentState: SparkApplicationState{CurrentStateSummary: "RunningHealthy"},
	}

	if shouldHandleSparkApplicationUpdate(oldStatus, newStatus) {
		t.Fatal("expected non-terminal update to be ignored")
	}
}
