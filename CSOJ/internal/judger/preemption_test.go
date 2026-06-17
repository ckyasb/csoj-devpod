package judger

import (
	"testing"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
)

func TestPreemptionReleasesOnlyPreemptedOwnerResources(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 1, Memory: 100}},
		}},
		Scheduler: config.Scheduler{
			QOS: []config.QOS{
				{Name: "normal", Priority: 1},
				{Name: "urgent", Priority: 10, Preempt: []string{"normal"}},
			},
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	scheduler.appState.Problems["low-problem"] = &Problem{ID: "low-problem", CPU: 1, Memory: 100}
	scheduler.appState.Problems["high-problem"] = &Problem{ID: "high-problem", CPU: 1, Memory: 100}

	user := models.User{ID: "u1", Username: "u1"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	low := models.Submission{
		ID:             "low",
		UserID:         user.ID,
		ProblemID:      "low-problem",
		Status:         models.StatusRunning,
		Cluster:        "debug",
		Node:           "n1",
		AllocatedCores: "0",
		QOS:            "normal",
	}
	if err := db.Create(&low).Error; err != nil {
		t.Fatalf("create low submission: %v", err)
	}

	node := scheduler.clusters["debug"].Nodes["n1"]
	node.Lock()
	node.UsedCores[0] = true
	node.UsedCoreOwners[0] = low.ID
	node.UsedMemory = 100
	node.MemoryAllocations[low.ID] = 100
	node.Unlock()

	high := QueuedSubmission{
		Submission: &models.Submission{
			ID:        "high",
			UserID:    user.ID,
			ProblemID: "high-problem",
			Status:    models.StatusQueued,
			Cluster:   "debug",
			QOS:       "urgent",
		},
		Problem: scheduler.appState.Problems["high-problem"],
	}

	allocatedNode, cores, preempted := scheduler.tryPreemptForJob("debug", high, now)
	if !preempted {
		t.Fatalf("expected urgent job to preempt normal job")
	}
	if allocatedNode == nil || allocatedNode.Name != "n1" || len(cores) != 1 || cores[0] != 0 {
		t.Fatalf("expected high job to allocate n1 core 0, got node=%v cores=%v", allocatedNode, cores)
	}

	var updatedLow models.Submission
	if err := db.First(&updatedLow, "id = ?", low.ID).Error; err != nil {
		t.Fatalf("load low submission: %v", err)
	}
	if updatedLow.Status != models.StatusFailed || updatedLow.Reason != "Preempted" {
		t.Fatalf("low submission should be failed/preempted, got status=%s reason=%q", updatedLow.Status, updatedLow.Reason)
	}

	scheduler.ReleaseResources("debug", "n1", []int{0}, 100, low.ID)
	node.Lock()
	defer node.Unlock()
	if !node.UsedCores[0] || node.UsedCoreOwners[0] != high.Submission.ID {
		t.Fatalf("old release should not free high job core, used=%v owner=%q", node.UsedCores[0], node.UsedCoreOwners[0])
	}
	if node.UsedMemory != 100 {
		t.Fatalf("old release should not subtract high memory allocation, got %d", node.UsedMemory)
	}
}

func TestPreemptionRequeuesRequeueableSubmission(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 1, Memory: 100}},
		}},
		Scheduler: config.Scheduler{
			QOS: []config.QOS{
				{Name: "normal", Priority: 1},
				{Name: "urgent", Priority: 10, Preempt: []string{"normal"}},
			},
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	mailer := &fakeMailSender{}
	scheduler.SetMailSender(mailer)
	scheduler.appState.Problems["low-problem"] = &Problem{ID: "low-problem", Cluster: "debug", CPU: 1, Memory: 100}
	scheduler.appState.Problems["high-problem"] = &Problem{ID: "high-problem", Cluster: "debug", CPU: 1, Memory: 100}

	user := models.User{ID: "u1", Username: "u1"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	low := models.Submission{
		ID:             "low-requeue",
		UserID:         user.ID,
		ProblemID:      "low-problem",
		Status:         models.StatusRunning,
		Cluster:        "debug",
		Node:           "n1",
		AllocatedCores: "0",
		QOS:            "normal",
		Requeue:        true,
		MailType:       "REQUEUE",
		MailUser:       "ops@example.com",
		CurrentStep:    1,
		Score:          50,
		Performance:    1.5,
		Info:           models.JSONMap{"old": "run"},
	}
	if err := db.Create(&low).Error; err != nil {
		t.Fatalf("create low submission: %v", err)
	}
	if err := db.Create(&models.Container{ID: "old-container", SubmissionID: low.ID, UserID: user.ID, Status: models.StatusRunning}).Error; err != nil {
		t.Fatalf("create old container: %v", err)
	}

	node := scheduler.clusters["debug"].Nodes["n1"]
	node.Lock()
	node.UsedCores[0] = true
	node.UsedCoreOwners[0] = low.ID
	node.UsedMemory = 100
	node.MemoryAllocations[low.ID] = 100
	node.Unlock()

	high := QueuedSubmission{
		Submission: &models.Submission{
			ID:        "high",
			UserID:    user.ID,
			ProblemID: "high-problem",
			Status:    models.StatusQueued,
			Cluster:   "debug",
			QOS:       "urgent",
		},
		Problem: scheduler.appState.Problems["high-problem"],
	}

	allocatedNode, cores, preempted := scheduler.tryPreemptForJob("debug", high, now)
	if !preempted || allocatedNode == nil || len(cores) != 1 {
		t.Fatalf("expected urgent job to preempt and allocate, node=%v cores=%v preempted=%v", allocatedNode, cores, preempted)
	}

	var updatedLow models.Submission
	if err := db.First(&updatedLow, "id = ?", low.ID).Error; err != nil {
		t.Fatalf("load low submission: %v", err)
	}
	if updatedLow.Status != models.StatusQueued || updatedLow.Node != "" || updatedLow.AllocatedCores != "" || updatedLow.Reason != "" {
		t.Fatalf("requeueable preempted submission should be queued and reset: %#v", updatedLow)
	}
	if updatedLow.CurrentStep != 0 || updatedLow.Score != 0 || updatedLow.Performance != 0 || len(updatedLow.Info) != 0 {
		t.Fatalf("requeued submission runtime fields were not reset: %#v", updatedLow)
	}

	var containers int64
	if err := db.Model(&models.Container{}).Where("submission_id = ?", low.ID).Count(&containers).Error; err != nil {
		t.Fatalf("count containers: %v", err)
	}
	if containers != 0 {
		t.Fatalf("old containers should be deleted on requeue, got %d", containers)
	}

	var events []models.AccountingRecord
	if err := db.Where("submission_id = ? AND event IN ?", low.ID, []string{database.AccountEventPreempted, database.AccountEventRequeued}).Order("id asc").Find(&events).Error; err != nil {
		t.Fatalf("load accounting events: %v", err)
	}
	if len(events) != 2 || events[0].Event != database.AccountEventPreempted || events[1].Event != database.AccountEventRequeued {
		t.Fatalf("expected preempted then requeued accounting events, got %#v", events)
	}
	waitForMailEvents(t, mailer, []SlurmMailEvent{SlurmMailRequeue})
}
