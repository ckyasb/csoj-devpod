package judger

import (
	"testing"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
)

func TestFairshareDecayPenalizesRecentBillingUsage(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			PriorityWeights: config.PriorityWeights{Fairshare: 10},
			FairshareDecay: config.FairshareDecay{
				Enabled:       true,
				HalfLifeHours: 1,
				UsageWeight:   10,
			},
			Accounts: []config.Account{
				{Name: "used", Fairshare: 10},
				{Name: "unused", Fairshare: 10},
			},
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	records := []models.AccountingRecord{
		{
			CreatedAt:    now.Add(-30 * time.Minute),
			Account:      "used",
			Event:        database.AccountEventCompleted,
			BillingUnits: 100,
		},
		{
			CreatedAt:    now.Add(-12 * time.Hour),
			Account:      "used",
			Event:        database.AccountEventCompleted,
			BillingUnits: 100,
		},
	}
	if err := db.Create(&records).Error; err != nil {
		t.Fatalf("create accounting records: %v", err)
	}

	used := QueuedSubmission{
		Submission: &models.Submission{ID: "used", Account: "used", CreatedAt: now},
		Problem:    &Problem{CPU: 1},
	}
	unused := QueuedSubmission{
		Submission: &models.Submission{ID: "unused", Account: "unused", CreatedAt: now},
		Problem:    &Problem{CPU: 1},
	}
	if scheduler.jobPriority("debug", used, now) >= scheduler.jobPriority("debug", unused, now) {
		t.Fatalf("account with recent billing usage should have lower priority")
	}

	recentPenalty := scheduler.fairshareUsagePenalty("used", now)
	laterPenalty := scheduler.fairshareUsagePenalty("used", now.Add(2*time.Hour))
	if laterPenalty >= recentPenalty {
		t.Fatalf("fairshare penalty should decay over time, recent=%d later=%d", recentPenalty, laterPenalty)
	}
}

func TestFairshareCountsReleasedInteractiveAllocationUsage(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			FairshareDecay: config.FairshareDecay{
				Enabled:       true,
				HalfLifeHours: 1,
				UsageWeight:   1,
			},
			Accounts: []config.Account{{Name: "interactive", Fairshare: 10}},
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	record := models.AccountingRecord{
		CreatedAt:    now,
		Account:      "interactive",
		Event:        database.AccountEventAllocationReleased,
		BillingUnits: 100,
	}
	if err := db.Create(&record).Error; err != nil {
		t.Fatalf("create accounting record: %v", err)
	}

	if penalty := scheduler.fairshareUsagePenalty("interactive", now); penalty == 0 {
		t.Fatalf("released interactive allocation should produce fairshare penalty")
	}
}
