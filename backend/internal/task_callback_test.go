package internal

import "testing"

func TestBuildTaskProcedureCallbackURL(t *testing.T) {
	url := BuildTaskProcedureCallbackURL("http://lakehouse-admin:8081/", 42)

	if got, want := url, "http://lakehouse-admin:8081/api/tasks/42/callback-result"; got != want {
		t.Fatalf("expected callback url %q, got %q", want, got)
	}
}

func TestMergeTaskResult(t *testing.T) {
	existing := map[string]any{
		"tracking_id": "app-1",
		"status":      "submitted",
	}
	update := map[string]any{
		"procedure": map[string]any{
			"query": "CALL x",
		},
	}

	merged := mergeTaskResult(existing, update)

	if got := merged["tracking_id"]; got != "app-1" {
		t.Fatalf("expected tracking_id to be preserved, got %#v", got)
	}

	procedure, ok := merged["procedure"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested procedure result, got %#v", merged["procedure"])
	}

	if got := procedure["query"]; got != "CALL x" {
		t.Fatalf("expected procedure query to be merged, got %#v", got)
	}
}
