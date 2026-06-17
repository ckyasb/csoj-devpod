package judger

import (
	"math"
	"testing"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
)

func TestCalculateBillingUsesCPUMemoryGRESTRESWeights(t *testing.T) {
	cfg := &config.Config{
		Scheduler: config.Scheduler{
			BillingWeights: map[string]float64{
				"cpu":         1,
				"mem":         0.001,
				"gpu":         10,
				"license/foo": 2,
			},
		},
	}
	problem := &Problem{
		CPU:    2,
		Memory: 1024,
		Scheduling: SchedulingConfig{
			GRES: "gpu:2",
			TRES: "license/foo:3",
		},
	}

	got := CalculateBilling(cfg, problem, nil)
	want := 2.0 + 1.024 + 20.0 + 6.0
	if math.Abs(got-want) > 0.0001 {
		t.Fatalf("expected billing %.3f, got %.3f", want, got)
	}

	got = CalculateBilling(cfg, problem, &models.Submission{CPU: 4, Memory: 2048})
	want = 4.0 + 2.048 + 20.0 + 6.0
	if math.Abs(got-want) > 0.0001 {
		t.Fatalf("expected override resource billing %.3f, got %.3f", want, got)
	}

	got = CalculateBilling(cfg, problem, &models.Submission{CPU: 1, Memory: 512, Nodes: 2})
	want = 2.0 + 1.024 + 20.0 + 6.0
	if math.Abs(got-want) > 0.0001 {
		t.Fatalf("expected multi-node billing %.3f, got %.3f", want, got)
	}
}

func TestEffectiveBillingUsesSubmissionOverride(t *testing.T) {
	got := EffectiveBilling(&config.Config{}, &Problem{CPU: 10}, &models.Submission{BillingUnits: 42})
	if got != 42 {
		t.Fatalf("expected override billing 42, got %.3f", got)
	}
}
