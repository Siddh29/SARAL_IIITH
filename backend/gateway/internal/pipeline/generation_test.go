package pipeline

import (
	"reflect"
	"testing"
)

func TestNormalizeGenerationFormats(t *testing.T) {
	for _, formats := range [][]string{nil, {}, {"poster"}, {"video", ""}, {"video", "podcast", "slides", "poster"}} {
		if _, err := normalizeGenerationFormats(formats); err == nil {
			t.Fatalf("expected invalid selection: %v", formats)
		}
	}
	got, err := normalizeGenerationFormats([]string{"video", "slides", "podcast"})
	if err != nil || !reflect.DeepEqual(got, []string{"podcast", "slides", "video"}) {
		t.Fatalf("canonical selection: %v, %v", got, err)
	}
	got, err = normalizeGenerationFormats([]string{"video", "video"})
	if err != nil || !reflect.DeepEqual(got, []string{"video"}) {
		t.Fatalf("deduplicate: %v, %v", got, err)
	}
}
