package models

import (
	"github.com/google/uuid"
	"time"
)

type GenerationOutput struct {
	RunID        uuid.UUID `json:"run_id"`
	Format       string    `json:"format"`
	Status       string    `json:"status"`
	CurrentStep  string    `json:"current_step"`
	ErrorMessage string    `json:"error_message,omitempty"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type GenerationBatch struct {
	ID          uuid.UUID          `json:"id"`
	SourceRunID uuid.UUID          `json:"source_run_id"`
	PaperID     uuid.UUID          `json:"paper_id"`
	Formats     []string           `json:"formats"`
	Status      string             `json:"status"`
	Outputs     []GenerationOutput `json:"outputs"`
}

// A failed output never cancels its siblings. Partial failures become terminal
// only after every selected format finishes (or fails).
func GenerationStatus(outputs []GenerationOutput) string {
	completed, failed := 0, 0
	for _, output := range outputs {
		switch output.Status {
		case "completed":
			completed++
		case "failed":
			failed++
		default:
			return "processing"
		}
	}
	if len(outputs) == 0 {
		return "pending"
	}
	if failed == len(outputs) {
		return "failed"
	}
	if completed == len(outputs) {
		return "completed"
	}
	return "partial_failure"
}
