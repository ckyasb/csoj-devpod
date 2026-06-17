package judger

import (
	"testing"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newPolicyTestScheduler(t *testing.T, cfg config.Config) (*Scheduler, *gorm.DB) {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sqlite db handle: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	if err := db.AutoMigrate(
		&models.User{},
		&models.Submission{},
		&models.Container{},
		&models.Allocation{},
		&models.RunStep{},
		&models.AccountingRecord{},
		&models.ContestScoreHistory{},
		&models.UserProblemBestScore{},
	); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	appState := &AppState{
		Contests:            map[string]*Contest{},
		Problems:            map[string]*Problem{},
		ProblemToContestMap: map[string]*Contest{},
	}
	return NewScheduler(&cfg, db, appState), db
}

func TestJobPriorityUsesPartitionQOSAgeNiceAndJobSize(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:         "debug",
			PriorityTier: 2,
			Nodes:        []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			PriorityWeights: config.PriorityWeights{
				Age:       1,
				QOS:       10,
				Nice:      2,
				Partition: 100,
				JobSize:   3,
			},
			QOS: []config.QOS{
				{Name: "normal", Priority: 1},
				{Name: "urgent", Priority: 20},
			},
		},
	}
	scheduler, _ := newPolicyTestScheduler(t, cfg)

	normal := QueuedSubmission{
		Submission: &models.Submission{ID: "normal", QOS: "normal", Nice: 5, CreatedAt: now.Add(-10 * time.Minute)},
		Problem:    &Problem{CPU: 1},
	}
	urgent := QueuedSubmission{
		Submission: &models.Submission{ID: "urgent", QOS: "urgent", CreatedAt: now},
		Problem:    &Problem{CPU: 2},
	}

	if scheduler.jobPriority("debug", urgent, now) <= scheduler.jobPriority("debug", normal, now) {
		t.Fatalf("urgent QoS job should outrank normal job")
	}
}

func TestJobPriorityUsesAccountFairshare(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			PriorityWeights: config.PriorityWeights{Fairshare: 10},
			Accounts: []config.Account{
				{Name: "low", Fairshare: 1},
				{Name: "high", Fairshare: 20},
			},
		},
	}
	scheduler, _ := newPolicyTestScheduler(t, cfg)

	low := QueuedSubmission{
		Submission: &models.Submission{ID: "low", Account: "low", CreatedAt: now},
		Problem:    &Problem{CPU: 1},
	}
	high := QueuedSubmission{
		Submission: &models.Submission{ID: "high", Account: "high", CreatedAt: now},
		Problem:    &Problem{CPU: 1},
	}

	if scheduler.jobPriority("debug", high, now) <= scheduler.jobPriority("debug", low, now) {
		t.Fatalf("higher fairshare account should receive higher priority")
	}
}

func TestNodeSelectionHonorsFeaturesGRESAndReservations(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name: "gpu",
			Nodes: []config.Node{
				{Name: "cpu-1", CPU: 4, Memory: 4096, Features: []string{"avx"}, Weight: 1},
				{Name: "gpu-1", CPU: 4, Memory: 4096, Features: []string{"gpu", "avx"}, GRES: []string{"gpu:2"}, Weight: 2},
			},
		}},
		Scheduler: config.Scheduler{
			Reservations: []config.Reservation{{
				Name:      "maintenance",
				Cluster:   "gpu",
				Nodes:     []string{"gpu-1"},
				StartTime: now.Add(-time.Hour),
				EndTime:   now.Add(time.Hour),
			}},
		},
	}
	scheduler, _ := newPolicyTestScheduler(t, cfg)

	gpuJob := QueuedSubmission{
		Submission: &models.Submission{ID: "gpu", Constraint: "gpu&avx", GRES: "gpu:1"},
		Problem:    &Problem{CPU: 1, Memory: 256},
	}
	node, _ := scheduler.findAvailableNodeForJob("gpu", gpuJob, now)
	if node != nil {
		t.Fatalf("unreserved job should not be scheduled onto an active exclusive reservation")
	}

	gpuJob.Submission.Reservation = "maintenance"
	node, cores := scheduler.findAvailableNodeForJob("gpu", gpuJob, now)
	if node == nil || node.Name != "gpu-1" {
		t.Fatalf("reserved gpu job should use gpu-1, got %#v", node)
	}
	if len(cores) != 1 {
		t.Fatalf("expected one allocated core, got %v", cores)
	}
}

func TestNodeListAndExcludeScheduling(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name: "debug",
			Nodes: []config.Node{
				{Name: "n01", CPU: 4, Memory: 1024},
				{Name: "n02", CPU: 4, Memory: 1024},
				{Name: "n03", CPU: 4, Memory: 1024},
			},
		}},
	}
	newScheduler := func(t *testing.T) *Scheduler {
		t.Helper()
		scheduler, _ := newPolicyTestScheduler(t, cfg)
		return scheduler
	}
	newJob := func(sub *models.Submission, scheduling SchedulingConfig) QueuedSubmission {
		return QueuedSubmission{
			Submission: sub,
			Problem:    &Problem{ID: "p1", Cluster: "debug", CPU: 1, Memory: 128, Scheduling: scheduling},
		}
	}

	t.Run("requested node list", func(t *testing.T) {
		scheduler := newScheduler(t)
		allocations := scheduler.findAvailableNodeAllocationsForJob("debug", newJob(&models.Submission{ID: "job-nodelist", NodeList: "n02"}, SchedulingConfig{}), time.Now())
		if allocationNodeList(allocations) != "n02" {
			t.Fatalf("allocation node list = %q, want n02", allocationNodeList(allocations))
		}
	})

	t.Run("excluded nodes", func(t *testing.T) {
		scheduler := newScheduler(t)
		allocations := scheduler.findAvailableNodeAllocationsForJob("debug", newJob(&models.Submission{ID: "job-exclude", ExcludeNodes: "n01"}, SchedulingConfig{}), time.Now())
		if allocationNodeList(allocations) != "n02" {
			t.Fatalf("allocation node list = %q, want n02", allocationNodeList(allocations))
		}
	})

	t.Run("bracket ranges", func(t *testing.T) {
		scheduler := newScheduler(t)
		allocations := scheduler.findAvailableNodeAllocationsForJob("debug", newJob(&models.Submission{ID: "job-bracket", NodeList: "n[02-03]"}, SchedulingConfig{}), time.Now())
		if allocationNodeList(allocations) != "n02" {
			t.Fatalf("allocation node list = %q, want n02", allocationNodeList(allocations))
		}
	})

	t.Run("problem defaults", func(t *testing.T) {
		scheduler := newScheduler(t)
		job := newJob(&models.Submission{ID: "job-problem-default"}, SchedulingConfig{NodeList: "n03"})
		allocations := scheduler.findAvailableNodeAllocationsForJob("debug", job, time.Now())
		if allocationNodeList(allocations) != "n03" {
			t.Fatalf("allocation node list = %q, want n03", allocationNodeList(allocations))
		}
	})

	t.Run("no matching requested node", func(t *testing.T) {
		scheduler := newScheduler(t)
		allocations := scheduler.findAvailableNodeAllocationsForJob("debug", newJob(&models.Submission{ID: "job-miss", NodeList: "n99"}, SchedulingConfig{}), time.Now())
		if len(allocations) != 0 {
			t.Fatalf("allocations = %#v, want none", allocations)
		}
	})
}

func TestReservationResourceCapsLimitAllocations(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			Reservations: []config.Reservation{
				{
					Name:      "small-cpu",
					Cluster:   "debug",
					Nodes:     []string{"n1"},
					StartTime: now.Add(-time.Hour),
					EndTime:   now.Add(time.Hour),
					CPU:       1,
					Memory:    1024,
				},
				{
					Name:      "small-mem",
					Cluster:   "debug",
					Nodes:     []string{"n1"},
					StartTime: now.Add(-time.Hour),
					EndTime:   now.Add(time.Hour),
					CPU:       4,
					Memory:    512,
				},
			},
		},
	}
	scheduler, _ := newPolicyTestScheduler(t, cfg)

	cpuJob := QueuedSubmission{
		Submission: &models.Submission{ID: "cpu-1", Reservation: "small-cpu"},
		Problem:    &Problem{CPU: 1, Memory: 256},
	}
	node, cores := scheduler.findAvailableNodeForJob("debug", cpuJob, now)
	if node == nil || len(cores) != 1 {
		t.Fatalf("first CPU reservation job should allocate, node=%v cores=%v", node, cores)
	}

	blockedCPUJob := QueuedSubmission{
		Submission: &models.Submission{ID: "cpu-2", Reservation: "small-cpu"},
		Problem:    &Problem{CPU: 1, Memory: 256},
	}
	node, _ = scheduler.findAvailableNodeForJob("debug", blockedCPUJob, now)
	if node != nil {
		t.Fatalf("second CPU reservation job should be blocked by reservation cap")
	}

	scheduler.ReleaseResources("debug", "n1", cores, 256, "cpu-1")
	node, cores = scheduler.findAvailableNodeForJob("debug", blockedCPUJob, now)
	if node == nil || len(cores) != 1 {
		t.Fatalf("CPU reservation cap should be released, node=%v cores=%v", node, cores)
	}
	scheduler.ReleaseResources("debug", "n1", cores, 256, "cpu-2")

	memJob := QueuedSubmission{
		Submission: &models.Submission{ID: "mem-1", Reservation: "small-mem"},
		Problem:    &Problem{CPU: 1, Memory: 512},
	}
	node, cores = scheduler.findAvailableNodeForJob("debug", memJob, now)
	if node == nil || len(cores) != 1 {
		t.Fatalf("first memory reservation job should allocate, node=%v cores=%v", node, cores)
	}

	blockedMemJob := QueuedSubmission{
		Submission: &models.Submission{ID: "mem-2", Reservation: "small-mem"},
		Problem:    &Problem{CPU: 1, Memory: 1},
	}
	node, _ = scheduler.findAvailableNodeForJob("debug", blockedMemJob, now)
	if node != nil {
		t.Fatalf("second memory reservation job should be blocked by reservation cap")
	}
}

func TestExclusiveJobReservesWholeNode(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
	}
	scheduler, _ := newPolicyTestScheduler(t, cfg)
	now := time.Now()

	sharedJob := QueuedSubmission{
		Submission: &models.Submission{ID: "shared"},
		Problem:    &Problem{CPU: 1, Memory: 128},
	}
	node, cores := scheduler.findAvailableNodeForJob("debug", sharedJob, now)
	if node == nil || len(cores) != 1 {
		t.Fatalf("shared job should allocate, node=%v cores=%v", node, cores)
	}

	exclusiveJob := QueuedSubmission{
		Submission: &models.Submission{ID: "exclusive", Exclusive: true},
		Problem:    &Problem{CPU: 1, Memory: 128},
	}
	node, _ = scheduler.findAvailableNodeForJob("debug", exclusiveJob, now)
	if node != nil {
		t.Fatalf("exclusive job should wait for a completely idle node")
	}

	scheduler.ReleaseResources("debug", "n1", cores, 128, "shared")
	node, cores = scheduler.findAvailableNodeForJob("debug", exclusiveJob, now)
	if node == nil || len(cores) != 4 || node.ExclusiveOwner != "exclusive" {
		t.Fatalf("exclusive job should reserve every core, node=%v cores=%v owner=%q", node, cores, node.ExclusiveOwner)
	}

	zeroCPUJob := QueuedSubmission{
		Submission: &models.Submission{ID: "zero-cpu"},
		Problem:    &Problem{CPU: 0, Memory: 0},
	}
	node, _ = scheduler.findAvailableNodeForJob("debug", zeroCPUJob, now)
	if node != nil {
		t.Fatalf("non-exclusive zero-cpu job should not share an exclusive node")
	}

	scheduler.ReleaseResources("debug", "n1", cores, 128, "exclusive")
	node, _ = scheduler.findAvailableNodeForJob("debug", zeroCPUJob, now)
	if node == nil {
		t.Fatalf("node should accept new jobs after exclusive release")
	}
}

func TestInteractiveAllocationUsesSchedulingPolicy(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			QOS: []config.QOS{{Name: "normal", MaxCPUPerJob: 1}},
			Reservations: []config.Reservation{{
				Name:      "staff",
				Cluster:   "debug",
				Nodes:     []string{"n1"},
				Users:     []string{"alice"},
				StartTime: now.Add(-time.Hour),
				EndTime:   now.Add(time.Hour),
			}},
		},
	}
	scheduler, _ := newPolicyTestScheduler(t, cfg)

	_, err := scheduler.AllocateInteractive(InteractiveAllocationRequest{
		UserID:  "u1",
		Cluster: "debug",
		CPU:     2,
		Memory:  128,
		QOS:     "normal",
	})
	if err == nil || err.Error() != "QOSMaxCPUPerJob" {
		t.Fatalf("interactive allocation should honor qos limit, err=%v", err)
	}

	_, err = scheduler.AllocateInteractive(InteractiveAllocationRequest{
		UserID:      "bob",
		Cluster:     "debug",
		CPU:         1,
		Memory:      128,
		Reservation: "staff",
	})
	if err == nil || err.Error() != "ReservationUserLimit" {
		t.Fatalf("interactive allocation should honor reservation users, err=%v", err)
	}
}

func TestInteractiveAllocationsCountTowardQOSRunningLimits(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			QOS: []config.QOS{{Name: "normal", MaxJobsPerUser: 1}},
		},
	}
	scheduler, _ := newPolicyTestScheduler(t, cfg)

	allocation, err := scheduler.AllocateInteractive(InteractiveAllocationRequest{
		UserID:  "u1",
		Cluster: "debug",
		CPU:     1,
		Memory:  128,
		QOS:     "normal",
	})
	if err != nil {
		t.Fatalf("first interactive allocation should succeed: %v", err)
	}
	defer func() {
		if _, err := scheduler.ReleaseInteractiveAllocation(allocation.ID); err != nil {
			t.Fatalf("release allocation: %v", err)
		}
	}()

	_, err = scheduler.AllocateInteractive(InteractiveAllocationRequest{
		UserID:  "u1",
		Cluster: "debug",
		CPU:     1,
		Memory:  128,
		QOS:     "normal",
	})
	if err == nil || err.Error() != "QOSMaxJobsPerUser" {
		t.Fatalf("second interactive allocation should count active allocation toward qos job limit, err=%v", err)
	}

	batchJob := QueuedSubmission{
		Submission: &models.Submission{ID: "batch", UserID: "u1", Status: models.StatusQueued, Cluster: "debug", QOS: "normal"},
		Problem:    &Problem{ID: "p1", Cluster: "debug", CPU: 1, Memory: 128},
	}
	decision, reason := scheduler.evaluateJob("debug", batchJob, time.Now())
	if decision != jobDecisionWait || reason != "QOSMaxJobsPerUser" {
		t.Fatalf("batch job should count active allocation toward qos job limit, got %v %q", decision, reason)
	}
}

func TestInteractiveAllocationsCountTowardQOSSubmitLimits(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			QOS: []config.QOS{{Name: "normal", MaxSubmitJobsPerUser: 1}},
		},
	}
	scheduler, _ := newPolicyTestScheduler(t, cfg)

	allocation, err := scheduler.AllocateInteractive(InteractiveAllocationRequest{
		UserID:  "u1",
		Cluster: "debug",
		CPU:     1,
		Memory:  128,
		QOS:     "normal",
	})
	if err != nil {
		t.Fatalf("first interactive allocation should succeed: %v", err)
	}
	defer func() {
		if _, err := scheduler.ReleaseInteractiveAllocation(allocation.ID); err != nil {
			t.Fatalf("release allocation: %v", err)
		}
	}()

	_, err = scheduler.AllocateInteractive(InteractiveAllocationRequest{
		UserID:  "u1",
		Cluster: "debug",
		CPU:     1,
		Memory:  128,
		QOS:     "normal",
	})
	if err == nil || err.Error() != "QOSMaxSubmitJobsPerUser" {
		t.Fatalf("second interactive allocation should count active allocation toward qos submit limit, err=%v", err)
	}
}

func TestSubmissionResourceOverridesDriveScheduling(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			QOS: []config.QOS{{Name: "normal", MaxCPUPerJob: 2, MaxMemoryPerJob: 1024}},
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	if err := db.Create(&models.User{ID: "u1", Username: "u1"}).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	job := QueuedSubmission{
		Submission: &models.Submission{ID: "job", UserID: "u1", ProblemID: "p1", Status: models.StatusQueued, Cluster: "debug", QOS: "normal", CPU: 2, Memory: 1024},
		Problem:    &Problem{ID: "p1", CPU: 1, Memory: 128, Workflow: []WorkflowStep{{Timeout: 10}}},
	}

	decision, reason := scheduler.evaluateJob("debug", job, time.Now())
	if decision != jobDecisionRun || reason != "" {
		t.Fatalf("resource override within qos limits should run, got %v %q", decision, reason)
	}
	node, cores := scheduler.findAvailableNodeForJob("debug", job, time.Now())
	if node == nil || len(cores) != 2 || node.UsedMemory != 1024 {
		t.Fatalf("expected override resources to be allocated, node=%v cores=%v used_memory=%d", node, cores, node.UsedMemory)
	}

	job.Submission.ID = "job-too-large"
	job.Submission.CPU = 3
	decision, reason = scheduler.evaluateJob("debug", job, time.Now())
	if decision != jobDecisionWait || reason != "QOSMaxCPUPerJob" {
		t.Fatalf("override CPU should honor qos max, got %v %q", decision, reason)
	}
}

func TestMultiNodeBatchAllocationReservesAndReleasesAllNodes(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name: "debug",
			Nodes: []config.Node{
				{Name: "n1", CPU: 4, Memory: 1024},
				{Name: "n2", CPU: 4, Memory: 1024},
			},
		}},
	}
	scheduler, _ := newPolicyTestScheduler(t, cfg)
	job := QueuedSubmission{
		Submission: &models.Submission{ID: "multi", Nodes: 2, CPU: 3},
		Problem:    &Problem{ID: "p1", Cluster: "debug", Memory: 256},
	}

	allocations := scheduler.findAvailableNodeAllocationsForJob("debug", job, time.Now())
	if len(allocations) != 2 {
		t.Fatalf("allocations len = %d, want 2", len(allocations))
	}
	if allocationNodeList(allocations) != "n1,n2" || allocationNodeCoreMap(allocations) != "n1:0,1;n2:0" {
		t.Fatalf("unexpected allocations: nodes=%q cores=%q", allocationNodeList(allocations), allocationNodeCoreMap(allocations))
	}
	if scheduler.clusters["debug"].Nodes["n1"].UsedMemory != 256 || scheduler.clusters["debug"].Nodes["n2"].UsedMemory != 256 {
		t.Fatalf("memory was not reserved on both nodes")
	}

	scheduler.ReleaseResources("debug", allocationNodeList(allocations), allocations[0].Cores, 256, job.Submission.ID)
	if scheduler.clusters["debug"].Nodes["n1"].UsedMemory != 0 || scheduler.clusters["debug"].Nodes["n2"].UsedMemory != 0 {
		t.Fatalf("memory was not released on both nodes")
	}
	for _, nodeName := range []string{"n1", "n2"} {
		node := scheduler.clusters["debug"].Nodes[nodeName]
		for coreID, used := range node.UsedCores {
			if used {
				t.Fatalf("node %s core %d still allocated", nodeName, coreID)
			}
		}
	}
}

func TestMultiNodeInteractiveAllocationReservesAndReleasesAllNodes(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name: "debug",
			Nodes: []config.Node{
				{Name: "n1", CPU: 4, Memory: 1024},
				{Name: "n2", CPU: 4, Memory: 1024},
			},
		}},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)

	allocation, err := scheduler.AllocateInteractive(InteractiveAllocationRequest{
		UserID:  "u1",
		Cluster: "debug",
		CPU:     3,
		Memory:  256,
		Nodes:   2,
	})
	if err != nil {
		t.Fatalf("allocate multi-node interactive resources: %v", err)
	}
	if allocation.Node != "n1,n2" || allocation.Nodes != 2 || allocation.CPU != 3 || allocation.AllocatedCores != "0,1" || allocation.AllocatedNodeCores != "n1:0,1;n2:0" {
		t.Fatalf("unexpected multi-node allocation: %#v", allocation)
	}
	if scheduler.clusters["debug"].Nodes["n1"].UsedMemory != 256 || scheduler.clusters["debug"].Nodes["n2"].UsedMemory != 256 {
		t.Fatalf("memory was not reserved on both nodes")
	}

	var record models.AccountingRecord
	if err := db.Where("submission_id = ? AND event = ?", allocation.ID, database.AccountEventAllocated).First(&record).Error; err != nil {
		t.Fatalf("load allocation accounting record: %v", err)
	}
	if record.CPU != 3 || record.Memory != 512 {
		t.Fatalf("unexpected allocation accounting resources: cpu=%d memory=%d", record.CPU, record.Memory)
	}

	released, err := scheduler.ReleaseInteractiveAllocation(allocation.ID)
	if err != nil {
		t.Fatalf("release multi-node allocation: %v", err)
	}
	if released.Status != models.AllocationReleased {
		t.Fatalf("allocation was not released: %#v", released)
	}
	for _, nodeName := range []string{"n1", "n2"} {
		node := scheduler.clusters["debug"].Nodes[nodeName]
		if node.UsedMemory != 0 {
			t.Fatalf("node %s memory still allocated: %d", nodeName, node.UsedMemory)
		}
		for coreID, used := range node.UsedCores {
			if used {
				t.Fatalf("node %s core %d still allocated", nodeName, coreID)
			}
		}
	}
}

func TestDependencyEvaluation(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	user := models.User{ID: "u1", Username: "u1"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	successDep := models.Submission{ID: "ok", UserID: user.ID, ProblemID: "p1", Status: models.StatusSuccess}
	successDep2 := models.Submission{ID: "ok2", UserID: user.ID, ProblemID: "p1", Status: models.StatusSuccess}
	failedDep := models.Submission{ID: "bad", UserID: user.ID, ProblemID: "p1", Status: models.StatusFailed}
	runningDep := models.Submission{ID: "running", UserID: user.ID, ProblemID: "p1", Status: models.StatusRunning}
	runningSameName := models.Submission{ID: "running-same-name", UserID: user.ID, ProblemID: "p2", JobName: "train", Status: models.StatusRunning}
	queuedDep := models.Submission{ID: "queued", UserID: user.ID, ProblemID: "p1", Status: models.StatusQueued}
	depArraySuccess := models.Submission{ID: "dep-array-1", UserID: user.ID, ProblemID: "p1", Status: models.StatusSuccess, ArrayJobID: "dep-array", ArrayTaskID: 1}
	depArrayRunning := models.Submission{ID: "dep-array-2", UserID: user.ID, ProblemID: "p1", Status: models.StatusRunning, ArrayJobID: "dep-array", ArrayTaskID: 2}
	depArrayFailed := models.Submission{ID: "dep-array-3", UserID: user.ID, ProblemID: "p1", Status: models.StatusFailed, ArrayJobID: "dep-array", ArrayTaskID: 3}
	depArrayAllSuccess1 := models.Submission{ID: "dep-array-all-1", UserID: user.ID, ProblemID: "p1", Status: models.StatusSuccess, ArrayJobID: "dep-array-all", ArrayTaskID: 1}
	depArrayAllSuccess2 := models.Submission{ID: "dep-array-all-3", UserID: user.ID, ProblemID: "p1", Status: models.StatusSuccess, ArrayJobID: "dep-array-all", ArrayTaskID: 3}
	if err := db.Create(&[]models.Submission{successDep, successDep2, failedDep, runningDep, runningSameName, queuedDep, depArraySuccess, depArrayRunning, depArrayFailed, depArrayAllSuccess1, depArrayAllSuccess2}).Error; err != nil {
		t.Fatalf("create deps: %v", err)
	}

	sub := &models.Submission{ID: "job", UserID: user.ID, ProblemID: "p1", Status: models.StatusQueued, Dependencies: "afterok:ok"}
	decision, reason := scheduler.evaluateDependencies(sub)
	if decision != jobDecisionRun || reason != "" {
		t.Fatalf("afterok on success should run, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:ok:ok2"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionRun || reason != "" {
		t.Fatalf("afterok on multiple successes should run, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:dep-array-all"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionRun || reason != "" {
		t.Fatalf("afterok on successful array should run, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:dep-array"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionFail || reason != "DependencyNeverSatisfied" {
		t.Fatalf("afterok on array with failed task should fail, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterany:dep-array"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionWait || reason != "Dependency" {
		t.Fatalf("afterany on array with running task should wait, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:dep-array_1"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionRun || reason != "" {
		t.Fatalf("afterok on successful array task should run, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:dep-array_2"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionWait || reason != "Dependency" {
		t.Fatalf("afterok on running array task should wait, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:dep-array_3"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionFail || reason != "DependencyNeverSatisfied" {
		t.Fatalf("afterok on failed array task should fail, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:dep-array_[1,2]"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionWait || reason != "Dependency" {
		t.Fatalf("afterok on bracket array task selector should wait for running task, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:dep-array-all_[1-3:2]"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionRun || reason != "" {
		t.Fatalf("afterok on bracket range array task selector should run, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:ok:bad"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionFail || reason != "DependencyNeverSatisfied" {
		t.Fatalf("afterok with one failed dependency should fail, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:bad"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionFail || reason != "DependencyNeverSatisfied" {
		t.Fatalf("afterok on failed dependency should fail, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:bad?afterok:ok"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionRun || reason != "" {
		t.Fatalf("OR dependency should run once one alternative is satisfied, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:bad?afterok:running"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionWait || reason != "Dependency" {
		t.Fatalf("OR dependency should wait while one alternative can still become satisfied, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:bad?afternotok:ok"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionFail || reason != "DependencyNeverSatisfied" {
		t.Fatalf("OR dependency should fail once all alternatives are impossible, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:ok:bad?afterok:ok2"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionRun || reason != "" {
		t.Fatalf("OR dependency should allow an alternative group after another group fails, got %v %q", decision, reason)
	}

	sub.Dependencies = "afternotok:bad"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionRun || reason != "" {
		t.Fatalf("afternotok on failed dependency should run, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterany:ok:running"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionWait || reason != "Dependency" {
		t.Fatalf("afterany should wait for unfinished dependency, got %v %q", decision, reason)
	}

	sub.Dependencies = "after:queued"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionWait || reason != "Dependency" {
		t.Fatalf("after should wait for queued dependency to start, got %v %q", decision, reason)
	}

	sub.Dependencies = "after:running"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionRun || reason != "" {
		t.Fatalf("after should run once dependency has started, got %v %q", decision, reason)
	}

	sub.ArrayJobID = "target-array"
	sub.ArrayTaskID = 1
	sub.Dependencies = "aftercorr:dep-array"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionRun || reason != "" {
		t.Fatalf("aftercorr should run when corresponding dependency task succeeded, got %v %q", decision, reason)
	}

	sub.ArrayTaskID = 2
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionWait || reason != "Dependency" {
		t.Fatalf("aftercorr should wait for unfinished corresponding task, got %v %q", decision, reason)
	}

	sub.ArrayTaskID = 3
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionFail || reason != "DependencyNeverSatisfied" {
		t.Fatalf("aftercorr should fail when corresponding task failed, got %v %q", decision, reason)
	}

	sub.ArrayJobID = ""
	sub.ArrayTaskID = 0
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionFail || reason != "InvalidDependency" {
		t.Fatalf("aftercorr on non-array job should be invalid, got %v %q", decision, reason)
	}

	sub.Dependencies = "afterok:"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionFail || reason != "InvalidDependency" {
		t.Fatalf("empty dependency id should be invalid, got %v %q", decision, reason)
	}

	sub.Dependencies = "singleton"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionWait || reason != "Dependency" {
		t.Fatalf("singleton should wait while another job is running, got %v %q", decision, reason)
	}

	sub.ProblemID = "p1"
	sub.JobName = "unique"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionRun || reason != "" {
		t.Fatalf("singleton should use job_name over problem_id, got %v %q", decision, reason)
	}

	sub.ProblemID = "p3"
	sub.JobName = "train"
	decision, reason = scheduler.evaluateDependencies(sub)
	if decision != jobDecisionWait || reason != "Dependency" {
		t.Fatalf("singleton should wait on same job_name even for different problems, got %v %q", decision, reason)
	}
}

func TestEvaluateJobHonorsHoldBeginDeadlineAndQOSLimits(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:    "debug",
			MaxTime: 60,
			Nodes:   []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			QOS: []config.QOS{{Name: "short", MaxCPUPerJob: 1, MaxTime: 30}},
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	user := models.User{ID: "u1", Username: "u1"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	future := now.Add(time.Hour)
	past := now.Add(-time.Hour)
	job := QueuedSubmission{
		Submission: &models.Submission{
			ID:        "job",
			UserID:    user.ID,
			Status:    models.StatusQueued,
			QOS:       "short",
			BeginTime: &future,
		},
		Problem: &Problem{CPU: 1, Memory: 256, Workflow: []WorkflowStep{{Timeout: 10}}},
	}
	decision, reason := scheduler.evaluateJob("debug", job, now)
	if decision != jobDecisionWait || reason != "BeginTime" {
		t.Fatalf("future begin time should wait, got %v %q", decision, reason)
	}

	job.Submission.BeginTime = nil
	job.Submission.Deadline = &past
	decision, reason = scheduler.evaluateJob("debug", job, now)
	if decision != jobDecisionFail || reason != "Deadline" {
		t.Fatalf("expired deadline should fail, got %v %q", decision, reason)
	}

	job.Submission.Deadline = nil
	job.Problem.CPU = 2
	decision, reason = scheduler.evaluateJob("debug", job, now)
	if decision != jobDecisionWait || reason != "QOSMaxCPUPerJob" {
		t.Fatalf("qos CPU limit should wait with reason, got %v %q", decision, reason)
	}
}

func TestEvaluateJobHonorsArrayConcurrencyLimit(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	user := models.User{ID: "u1", Username: "u1"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	running := models.Submission{
		ID:              "array-running",
		UserID:          user.ID,
		ProblemID:       "p1",
		Status:          models.StatusRunning,
		ArrayJobID:      "array-1",
		ArrayTaskID:     0,
		ArrayMaxRunning: 1,
	}
	if err := db.Create(&running).Error; err != nil {
		t.Fatalf("create running array task: %v", err)
	}

	job := QueuedSubmission{
		Submission: &models.Submission{
			ID:              "array-waiting",
			UserID:          user.ID,
			ProblemID:       "p1",
			Status:          models.StatusQueued,
			ArrayJobID:      "array-1",
			ArrayTaskID:     1,
			ArrayMaxRunning: 1,
		},
		Problem: &Problem{CPU: 1, Memory: 256, Workflow: []WorkflowStep{{Timeout: 10}}},
	}
	decision, reason := scheduler.evaluateJob("debug", job, now)
	if decision != jobDecisionWait || reason != "ArrayTaskLimit" {
		t.Fatalf("array concurrency limit should wait, got %v %q", decision, reason)
	}
}

func TestEvaluateJobHonorsAccountLimits(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			Accounts: []config.Account{{Name: "course-a", MaxJobs: 1, MaxSubmit: 2}},
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	user := models.User{ID: "u1", Username: "u1"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	running := models.Submission{ID: "running", UserID: user.ID, ProblemID: "p1", Status: models.StatusRunning, Account: "course-a"}
	queued := models.Submission{ID: "queued", UserID: user.ID, ProblemID: "p1", Status: models.StatusQueued, Account: "course-a"}
	if err := db.Create(&[]models.Submission{running, queued}).Error; err != nil {
		t.Fatalf("create existing submissions: %v", err)
	}

	job := QueuedSubmission{
		Submission: &models.Submission{
			ID:        "candidate",
			UserID:    user.ID,
			ProblemID: "p1",
			Status:    models.StatusQueued,
			Account:   "course-a",
		},
		Problem: &Problem{CPU: 1, Memory: 256, Workflow: []WorkflowStep{{Timeout: 10}}},
	}
	decision, reason := scheduler.evaluateJob("debug", job, now)
	if decision != jobDecisionWait || reason != "AccountMaxJobs" {
		t.Fatalf("account max jobs should wait, got %v %q", decision, reason)
	}
}

func TestEvaluateJobHonorsBillingLimits(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 4, Memory: 4096}},
		}},
		Scheduler: config.Scheduler{
			BillingWeights: map[string]float64{"cpu": 1},
			Accounts: []config.Account{{
				Name:              "course-a",
				MaxBillingRunning: 10,
			}},
			QOS: []config.QOS{{
				Name:             "normal",
				MaxBillingPerJob: 6,
			}},
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	user := models.User{ID: "u1", Username: "u1"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	running := models.Submission{
		ID:           "running",
		UserID:       user.ID,
		ProblemID:    "p1",
		Status:       models.StatusRunning,
		Account:      "course-a",
		BillingUnits: 8,
	}
	if err := db.Create(&running).Error; err != nil {
		t.Fatalf("create running submission: %v", err)
	}

	job := QueuedSubmission{
		Submission: &models.Submission{
			ID:        "candidate",
			UserID:    user.ID,
			ProblemID: "p1",
			Status:    models.StatusQueued,
			Account:   "course-a",
			QOS:       "normal",
		},
		Problem: &Problem{CPU: 5, Memory: 256, Workflow: []WorkflowStep{{Timeout: 10}}},
	}
	decision, reason := scheduler.evaluateJob("debug", job, now)
	if decision != jobDecisionWait || reason != "AccountBillingLimit" {
		t.Fatalf("account billing limit should wait, got %v %q", decision, reason)
	}

	job.Submission.Account = ""
	job.Problem.CPU = 7
	decision, reason = scheduler.evaluateJob("debug", job, now)
	if decision != jobDecisionWait || reason != "QOSMaxBillingPerJob" {
		t.Fatalf("qos billing-per-job limit should wait, got %v %q", decision, reason)
	}
}
