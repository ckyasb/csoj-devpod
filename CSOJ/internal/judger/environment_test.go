package judger

import (
	"reflect"
	"testing"

	"github.com/ZJUSCT/CSOJ/internal/database/models"
)

func TestSubmissionEnvironmentVariables(t *testing.T) {
	sub := &models.Submission{
		Environment: models.JSONMap{
			"Z_VAR":    "last",
			"A_VAR":    42,
			"BAD-NAME": "skip",
		},
	}

	got := SubmissionEnvironmentVariables(sub)
	want := []string{"A_VAR=42", "Z_VAR=last"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SubmissionEnvironmentVariables() = %#v, want %#v", got, want)
	}
}
