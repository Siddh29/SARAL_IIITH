package models

import "testing"

func TestGenerationStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		states []string
		want   string
	}{
		{"empty", nil, "pending"},
		{"all ready", []string{"completed", "completed", "completed"}, "completed"},
		{"partial failure", []string{"completed", "failed", "completed"}, "partial_failure"},
		{"failure does not cancel siblings", []string{"failed", "processing", "completed"}, "processing"},
		{"all failed", []string{"failed", "failed"}, "failed"},
		{"queued", []string{"pending", "completed"}, "processing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outputs := []GenerationOutput{}
			for _, state := range tc.states {
				outputs = append(outputs, GenerationOutput{Status: state})
			}
			if got := GenerationStatus(outputs); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
