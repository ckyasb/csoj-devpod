package judger

import (
	"testing"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
)

func TestLicensePoolAcquireAndRelease(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			Licenses: map[string]int{"license/foo": 2},
		},
	}
	scheduler, _ := newPolicyTestScheduler(t, cfg)
	job := QueuedSubmission{
		Submission: &models.Submission{ID: "job-1"},
		Problem:    &Problem{Scheduling: SchedulingConfig{TRES: "license/foo:2"}},
	}

	if !scheduler.AcquireLicensesForJob(job, "job-1") {
		t.Fatalf("expected license acquisition to succeed")
	}
	status := scheduler.GetLicenseStatus()
	if len(status) != 1 || status[0].Used != 2 || status[0].Available != 0 || status[0].Owners["job-1"] != 2 {
		t.Fatalf("unexpected license status after acquire: %#v", status)
	}
	if scheduler.AcquireLicensesForJob(QueuedSubmission{
		Submission: &models.Submission{ID: "job-2"},
		Problem:    &Problem{Scheduling: SchedulingConfig{TRES: "license/foo:1"}},
	}, "job-2") {
		t.Fatalf("expected second acquisition to fail while pool is exhausted")
	}

	scheduler.ReleaseResources("debug", "n1", nil, 0, "job-1")
	status = scheduler.GetLicenseStatus()
	if len(status) != 1 || status[0].Used != 0 || status[0].Available != 2 {
		t.Fatalf("unexpected license status after release: %#v", status)
	}
}

func TestSchedulePendingWaitsForUnavailableLicenses(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			Licenses: map[string]int{"license/foo": 1},
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	sub := models.Submission{ID: "job-1", Status: models.StatusQueued, ProblemID: "p1"}
	if err := db.Create(&sub).Error; err != nil {
		t.Fatalf("create submission: %v", err)
	}

	pending := scheduler.schedulePending("debug", []QueuedSubmission{{
		Submission: &sub,
		Problem:    &Problem{ID: "p1", CPU: 1, Memory: 128, Scheduling: SchedulingConfig{TRES: "license/foo:2"}},
	}})
	if len(pending) != 1 {
		t.Fatalf("job should remain pending, got %d pending jobs", len(pending))
	}

	var updated models.Submission
	if err := db.Where("id = ?", "job-1").First(&updated).Error; err != nil {
		t.Fatalf("load updated submission: %v", err)
	}
	if updated.Reason != "Licenses" {
		t.Fatalf("pending reason = %q, want Licenses", updated.Reason)
	}
}
