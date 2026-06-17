package judger

import "testing"

func TestParseJobArray(t *testing.T) {
	array, err := ParseJobArray("1-5:2,8,8%3")
	if err != nil {
		t.Fatalf("parse job array: %v", err)
	}

	expected := []int{1, 3, 5, 8}
	if array.MaxRunning != 3 {
		t.Fatalf("expected max running 3, got %d", array.MaxRunning)
	}
	if len(array.TaskIDs) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, array.TaskIDs)
	}
	for i := range expected {
		if array.TaskIDs[i] != expected[i] {
			t.Fatalf("expected %v, got %v", expected, array.TaskIDs)
		}
	}
}

func TestParseJobArrayRejectsInvalidSpecs(t *testing.T) {
	invalidSpecs := []string{
		"5-1",
		"1-5:0",
		"1-5%0",
		"abc",
	}
	for _, spec := range invalidSpecs {
		if _, err := ParseJobArray(spec); err == nil {
			t.Fatalf("expected %q to fail", spec)
		}
	}
}
