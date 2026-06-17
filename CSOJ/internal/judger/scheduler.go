package judger

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// AppState holds the shared, reloadable state of contests and problems.
type AppState struct {
	sync.RWMutex
	Contests            map[string]*Contest
	Problems            map[string]*Problem
	ProblemToContestMap map[string]*Contest
}

type NodeState struct {
	sync.Mutex
	*config.Node
	UsedMemory        int64            `json:"used_memory"`
	UsedCores         []bool           `json:"used_cores"`
	UsedCoreOwners    []string         `json:"-"`
	MemoryAllocations map[string]int64 `json:"-"`
	ExclusiveOwner    string           `json:"-"`
	IsPaused          bool             `json:"is_paused"`
}

type NodeDetail struct {
	*config.Node
	UsedMemory int64  `json:"used_memory"`
	UsedCores  []bool `json:"used_cores"`
	IsPaused   bool   `json:"is_paused"`
}

type ClusterState struct {
	sync.Mutex
	*config.Cluster
	Nodes map[string]*NodeState `json:"nodes"`
}

type QueuedSubmission struct {
	Submission *models.Submission
	Problem    *Problem
}

type QueueEntry struct {
	ID                 string        `json:"id"`
	UserID             string        `json:"user_id"`
	Problem            string        `json:"problem_id"`
	JobName            string        `json:"job_name"`
	Status             models.Status `json:"status"`
	Cluster            string        `json:"cluster"`
	Node               string        `json:"node"`
	AllocatedNodeCores string        `json:"allocated_node_cores"`
	QOS                string        `json:"qos"`
	Account            string        `json:"account"`
	CPU                int           `json:"cpu"`
	NTasks             int           `json:"ntasks"`
	CPUsPerTask        int           `json:"cpus_per_task"`
	Nodes              int           `json:"nodes"`
	Memory             int64         `json:"memory"`
	TRES               string        `json:"tres"`
	Licenses           string        `json:"licenses"`
	NodeList           string        `json:"nodelist"`
	ExcludeNodes       string        `json:"exclude_nodes"`
	BillingUnits       float64       `json:"billing_units"`
	Priority           int           `json:"priority"`
	Position           int           `json:"position"`
	Reason             string        `json:"reason"`
	SlurmState         string        `json:"slurm_state"`
	SlurmReason        string        `json:"slurm_reason"`
	ArrayJobID         string        `json:"array_job_id"`
	ArrayTaskID        int           `json:"array_task_id"`
	Created            time.Time     `json:"created_at"`
}

type RuntimeFactory func(config.Node) (RuntimeManager, error)

type reservationAllocation struct {
	Cluster     string
	Reservation string
	CPU         int
	Memory      int64
}

type nodeAllocation struct {
	Node   *NodeState
	Cores  []int
	CPU    int
	Memory int64
}

type Scheduler struct {
	cfg             *config.Config
	db              *gorm.DB
	clusters        map[string]*ClusterState
	appState        *AppState
	queues          map[string]chan QueuedSubmission
	configMu        sync.RWMutex
	pendingMu       sync.Mutex
	pendingLen      map[string]int
	licenseMu       sync.Mutex
	licenseTotals   map[string]int
	licenseUsed     map[string]int
	licenseOwners   map[string]map[string]int
	reservationMu   sync.Mutex
	reservationUsed map[string]reservationAllocation
	dispatcher      *Dispatcher
	runtimeFactory  RuntimeFactory
	mailSender      MailSender
}

func NewScheduler(cfg *config.Config, db *gorm.DB, appState *AppState) *Scheduler {
	clusters := make(map[string]*ClusterState)
	queues := make(map[string]chan QueuedSubmission)
	queueSize := cfg.Scheduler.QueueSize
	if queueSize <= 0 {
		queueSize = 1024
	}
	for i := range cfg.Cluster {
		cluster := cfg.Cluster[i]
		clusterState := &ClusterState{
			Cluster: &cluster,
			Nodes:   make(map[string]*NodeState),
		}
		for j := range cluster.Nodes {
			node := cluster.Nodes[j]
			// 初始化核心使用状态，所有核心都标记为未使用 (false)
			nodeCores := make([]bool, node.CPU)
			clusterState.Nodes[node.Name] = &NodeState{
				Node:              &node,
				UsedMemory:        0,
				UsedCores:         nodeCores,
				UsedCoreOwners:    make([]string, node.CPU),
				MemoryAllocations: make(map[string]int64),
				IsPaused:          false,
			}
		}
		clusters[cluster.Name] = clusterState
		queues[cluster.Name] = make(chan QueuedSubmission, queueSize)
	}

	scheduler := &Scheduler{
		cfg:             cfg,
		db:              db,
		clusters:        clusters,
		queues:          queues,
		appState:        appState,
		pendingLen:      make(map[string]int),
		reservationUsed: make(map[string]reservationAllocation),
		runtimeFactory:  NewRuntimeManager,
		mailSender:      NewMailSender(cfg.Mail),
	}
	scheduler.initLicenses(cfg.Scheduler.Licenses)
	scheduler.dispatcher = NewDispatcher(cfg, db, scheduler, appState)
	return scheduler
}

func (s *Scheduler) SetRuntimeFactory(factory RuntimeFactory) {
	if factory == nil {
		s.runtimeFactory = NewRuntimeManager
		return
	}
	s.runtimeFactory = factory
}

// RequeuePendingSubmissions loads submissions with 'Queued' status from the DB
// and adds them back to the scheduler's queue on startup.
func RequeuePendingSubmissions(db *gorm.DB, s *Scheduler, appState *AppState) error {
	var pendingSubs []models.Submission
	if err := db.Model(&models.Submission{}).Where("status = ?", models.StatusQueued).Order("created_at asc").Find(&pendingSubs).Error; err != nil {
		return err
	}

	if len(pendingSubs) == 0 {
		zap.S().Info("no pending submissions to requeue")
		return nil
	}

	zap.S().Infof("requeueing %d pending submissions...", len(pendingSubs))
	appState.RLock()
	defer appState.RUnlock()
	for _, sub := range pendingSubs {
		submission := sub // Create a new variable to avoid pointer issues with the loop variable
		problem, ok := appState.Problems[submission.ProblemID]
		if !ok {
			zap.S().Warnf("problem %s for submission %s not found, skipping requeue", submission.ProblemID, submission.ID)
			continue
		}
		s.Submit(&submission, problem)
	}
	zap.S().Info("finished requeueing pending submissions")
	return nil
}

func (s *Scheduler) GetClusterStates() map[string]ClusterState {
	snapshot := make(map[string]ClusterState)
	for name, cluster := range s.clusters {
		cluster.Lock()
		nodeSnapshots := make(map[string]*NodeState)
		for nodeName, node := range cluster.Nodes {
			node.Lock()
			// Create a copy to avoid exposing internal state directly
			nodeStateCopy := *node.Node
			nodeSnapshots[nodeName] = &NodeState{
				Node:              &nodeStateCopy,
				UsedMemory:        node.UsedMemory,
				IsPaused:          node.IsPaused,
				UsedCores:         append([]bool(nil), node.UsedCores...),
				UsedCoreOwners:    append([]string(nil), node.UsedCoreOwners...),
				MemoryAllocations: copyMemoryAllocations(node.MemoryAllocations),
				ExclusiveOwner:    node.ExclusiveOwner,
			}
			node.Unlock()
		}
		clusterConfigCopy := *cluster.Cluster
		snapshot[name] = ClusterState{
			Cluster: &clusterConfigCopy,
			Nodes:   nodeSnapshots,
		}
		cluster.Unlock()
	}
	return snapshot
}

func (s *Scheduler) GetNodeDetails(clusterName, nodeName string) (*NodeDetail, error) {
	cluster, ok := s.clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("cluster '%s' not found", clusterName)
	}

	node, ok := cluster.Nodes[nodeName]
	if !ok {
		return nil, fmt.Errorf("node '%s' not found in cluster '%s'", nodeName, clusterName)
	}

	node.Lock()
	defer node.Unlock()

	nodeConfigCopy := *node.Node
	details := &NodeDetail{
		Node:       &nodeConfigCopy,
		UsedMemory: node.UsedMemory,
		IsPaused:   node.IsPaused,
		UsedCores:  append([]bool(nil), node.UsedCores...), // Return a copy
	}

	return details, nil
}

func (s *Scheduler) ListPartitions(name string) []config.Cluster {
	partitions := make([]config.Cluster, 0, len(s.clusters))
	names := make([]string, 0, len(s.clusters))
	for clusterName := range s.clusters {
		names = append(names, clusterName)
	}
	sort.Strings(names)
	for _, clusterName := range names {
		if name != "" && !strings.EqualFold(clusterName, name) {
			continue
		}
		cluster := s.clusters[clusterName]
		cluster.Lock()
		partitions = append(partitions, cloneClusterConfig(*cluster.Cluster))
		cluster.Unlock()
	}
	return partitions
}

func (s *Scheduler) UpdatePartition(name string, update config.Cluster) (config.Cluster, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = strings.TrimSpace(update.Name)
	}
	if name == "" {
		return config.Cluster{}, fmt.Errorf("partition name is required")
	}
	cluster, ok := s.clusters[name]
	if !ok {
		return config.Cluster{}, fmt.Errorf("partition %q not found", name)
	}
	cluster.Lock()
	defer cluster.Unlock()
	cluster.Name = name
	if update.State != "" {
		cluster.State = update.State
	}
	cluster.PriorityTier = update.PriorityTier
	cluster.MaxTime = update.MaxTime
	cluster.MaxJobs = update.MaxJobs
	cluster.AllowUsers = append([]string(nil), update.AllowUsers...)
	cluster.AllowAccounts = append([]string(nil), update.AllowAccounts...)
	cluster.AllowQOS = append([]string(nil), update.AllowQOS...)
	cluster.DenyQOS = append([]string(nil), update.DenyQOS...)
	return cloneClusterConfig(*cluster.Cluster), nil
}

func (s *Scheduler) PauseNode(clusterName, nodeName string) error {
	cluster, ok := s.clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster '%s' not found", clusterName)
	}

	node, ok := cluster.Nodes[nodeName]
	if !ok {
		return fmt.Errorf("node '%s' not found in cluster '%s'", nodeName, clusterName)
	}

	node.Lock()
	defer node.Unlock()
	node.IsPaused = true
	if node.State == "" || node.State == "idle" || node.State == "up" {
		node.State = "drain"
	}
	if node.Reason == "" {
		node.Reason = "Paused"
	}
	zap.S().Warnf("admin paused node '%s/%s'", clusterName, nodeName)
	return nil
}

func (s *Scheduler) ResumeNode(clusterName, nodeName string) error {
	cluster, ok := s.clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster '%s' not found", clusterName)
	}

	node, ok := cluster.Nodes[nodeName]
	if !ok {
		return fmt.Errorf("node '%s' not found in cluster '%s'", nodeName, clusterName)
	}

	node.Lock()
	defer node.Unlock()
	node.IsPaused = false
	if node.State == "drain" || node.State == "drained" || node.State == "down" {
		node.State = "idle"
	}
	node.Reason = ""
	zap.S().Infof("admin resumed node '%s/%s'", clusterName, nodeName)
	return nil
}

func (s *Scheduler) SetNodeState(clusterName, nodeName, state, reason string) error {
	cluster, ok := s.clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster '%s' not found", clusterName)
	}

	node, ok := cluster.Nodes[nodeName]
	if !ok {
		return fmt.Errorf("node '%s' not found in cluster '%s'", nodeName, clusterName)
	}

	node.Lock()
	defer node.Unlock()
	node.State = state
	node.IsPaused = state == "drain" || state == "drained" || state == "down"
	if reason != "" || node.IsPaused {
		node.Reason = reason
	}
	if state == "idle" || state == "up" {
		node.Reason = ""
	}
	zap.S().Warnf("admin set node '%s/%s' state to '%s'", clusterName, nodeName, state)
	return nil
}

func (s *Scheduler) GetQueueLengths() map[string]int {
	lengths := make(map[string]int)
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	for name, queue := range s.queues {
		lengths[name] = len(queue) + s.pendingLen[name]
	}
	return lengths
}

func (s *Scheduler) GetQueueSnapshot() ([]QueueEntry, error) {
	var submissions []models.Submission
	if err := s.db.
		Where("status IN ?", []models.Status{models.StatusQueued, models.StatusRunning, models.StatusSuspended}).
		Order("created_at asc").
		Find(&submissions).Error; err != nil {
		return nil, err
	}

	positions := make(map[string]int)
	entries := make([]QueueEntry, 0, len(submissions))
	now := time.Now()
	s.appState.RLock()
	defer s.appState.RUnlock()
	for i := range submissions {
		sub := submissions[i]
		problem := s.appState.Problems[sub.ProblemID]
		entry := QueueEntry{
			ID:                 sub.ID,
			UserID:             sub.UserID,
			Problem:            sub.ProblemID,
			JobName:            sub.JobName,
			Status:             sub.Status,
			Cluster:            sub.Cluster,
			Node:               sub.Node,
			AllocatedNodeCores: sub.AllocatedNodeCores,
			QOS:                sub.QOS,
			Account:            sub.Account,
			CPU:                EffectiveCPU(problem, &sub),
			NTasks:             sub.NTasks,
			CPUsPerTask:        sub.CPUsPerTask,
			Nodes:              sub.Nodes,
			Memory:             EffectiveMemory(problem, &sub),
			TRES:               sub.TRES,
			Licenses:           sub.Licenses,
			NodeList:           sub.NodeList,
			ExcludeNodes:       sub.ExcludeNodes,
			BillingUnits:       sub.BillingUnits,
			Reason:             sub.Reason,
			ArrayJobID:         sub.ArrayJobID,
			ArrayTaskID:        sub.ArrayTaskID,
			Created:            sub.CreatedAt,
		}
		entry.SlurmState, entry.SlurmReason = models.DeriveSlurmJobState(sub.Status, sub.Hold, sub.Reason)
		if sub.Status == models.StatusQueued {
			positions[sub.Cluster]++
			entry.Position = positions[sub.Cluster]
		}
		if problem != nil {
			entry.Priority = s.jobPriority(sub.Cluster, QueuedSubmission{
				Submission: &sub,
				Problem:    problem,
			}, now)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (s *Scheduler) setPendingLength(clusterName string, length int) {
	s.pendingMu.Lock()
	s.pendingLen[clusterName] = length
	s.pendingMu.Unlock()
}

func (s *Scheduler) Submit(submission *models.Submission, problem *Problem) {
	clusterName := submission.Cluster
	if clusterName == "" {
		clusterName = problem.Cluster
	}
	if clusterName == "" {
		clusterName = s.defaultClusterName()
	}
	if queue, ok := s.queues[clusterName]; ok {
		submission.Cluster = clusterName
		queue <- QueuedSubmission{Submission: submission, Problem: problem}
		zap.S().Infof("submission %s for problem %s added to queue for cluster '%s'", submission.ID, problem.ID, clusterName)
	} else {
		zap.S().Errorf("submission %s for problem %s has an invalid cluster '%s', dropping", submission.ID, problem.ID, clusterName)
		// Mark submission as failed
		submission.Status = models.StatusFailed
		submission.Info = models.JSONMap{"error": "Invalid cluster specified in problem definition"}
		if err := s.db.Save(submission).Error; err != nil {
			zap.S().Errorf("failed to update submission %s status to failed: %v", submission.ID, err)
		}
	}
}

func (s *Scheduler) Run() {
	for clusterName, queue := range s.queues {
		go s.clusterWorker(clusterName, queue)
	}
}

func (s *Scheduler) clusterWorker(clusterName string, queue <-chan QueuedSubmission) {
	zap.S().Infof("starting worker for cluster '%s'", clusterName)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	pending := make([]QueuedSubmission, 0)
	for {
		select {
		case job, ok := <-queue:
			if !ok {
				return
			}
			pending = append(pending, job)
			s.setPendingLength(clusterName, len(pending))
			zap.S().Infof("submission %s for cluster '%s' entered pending queue", job.Submission.ID, clusterName)
		case <-ticker.C:
		}

	drainLoop:
		for {
			select {
			case job, ok := <-queue:
				if !ok {
					return
				}
				pending = append(pending, job)
			default:
				break drainLoop
			}
		}

		if len(pending) == 0 {
			s.setPendingLength(clusterName, 0)
			continue
		}

		pending = s.schedulePending(clusterName, pending)
		s.setPendingLength(clusterName, len(pending))
	}
}

func (s *Scheduler) schedulePending(clusterName string, pending []QueuedSubmission) []QueuedSubmission {
	for {
		pending = s.refreshPendingJobs(pending)
		if len(pending) == 0 {
			return pending
		}

		sort.SliceStable(pending, func(i, j int) bool {
			left := s.jobPriority(clusterName, pending[i], time.Now())
			right := s.jobPriority(clusterName, pending[j], time.Now())
			if left == right {
				return pending[i].Submission.CreatedAt.Before(pending[j].Submission.CreatedAt)
			}
			return left > right
		})

		scheduledOne := false
		now := time.Now()
		backfillEnabled := s.backfillEnabled()
		for idx, job := range pending {
			decision, reason := s.evaluateJob(clusterName, job, now)
			switch decision {
			case jobDecisionFail:
				s.failQueuedSubmission(job.Submission, reason)
				pending = removePendingAt(pending, idx)
				scheduledOne = true
				goto nextPass
			case jobDecisionWait:
				s.updatePendingReason(job.Submission, reason)
				if !backfillEnabled {
					return pending
				}
				continue
			}

			if !s.AcquireLicensesForJob(job, job.Submission.ID) {
				s.updatePendingReason(job.Submission, "Licenses")
				if !backfillEnabled {
					return pending
				}
				continue
			}
			licenseAcquired := true

			allocations := s.findAvailableNodeAllocationsForJob(clusterName, job, now)
			if len(allocations) == 0 {
				var preempted bool
				allocations, preempted = s.tryPreemptForJobAllocations(clusterName, job, now)
				if len(allocations) > 0 {
					if s.startJobWithAllocations(clusterName, job, allocations) {
						pending = removePendingAt(pending, idx)
						scheduledOne = true
						goto nextPass
					}
					licenseAcquired = false
				}
				if preempted {
					if licenseAcquired {
						s.ReleaseLicenses(job.Submission.ID)
					}
					scheduledOne = true
					goto nextPass
				}
				if licenseAcquired {
					s.ReleaseLicenses(job.Submission.ID)
				}
				s.updatePendingReason(job.Submission, "Resources")
				if !backfillEnabled {
					return pending
				}
				continue
			}

			if s.startJobWithAllocations(clusterName, job, allocations) {
				pending = removePendingAt(pending, idx)
				scheduledOne = true
				goto nextPass
			}
		}

	nextPass:
		if !scheduledOne {
			return pending
		}
	}
}

func (s *Scheduler) refreshPendingJobs(pending []QueuedSubmission) []QueuedSubmission {
	refreshed := pending[:0]
	for _, job := range pending {
		var currentSub models.Submission
		if err := s.db.First(&currentSub, "id = ?", job.Submission.ID).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				zap.S().Warnf("submission %s was deleted from DB, dropping job.", job.Submission.ID)
			} else {
				zap.S().Errorf("failed to refetch submission %s from DB: %v", job.Submission.ID, err)
				refreshed = append(refreshed, job)
			}
			continue
		}
		if currentSub.Status != models.StatusQueued {
			zap.S().Infof("submission %s is no longer queued (%s), dropping pending job", currentSub.ID, currentSub.Status)
			continue
		}
		job.Submission = &currentSub
		refreshed = append(refreshed, job)
	}
	return refreshed
}

func removePendingAt(pending []QueuedSubmission, idx int) []QueuedSubmission {
	copy(pending[idx:], pending[idx+1:])
	return pending[:len(pending)-1]
}

func (s *Scheduler) startJob(clusterName string, job QueuedSubmission, node *NodeState, allocatedCores []int) bool {
	return s.startJobWithAllocations(clusterName, job, []nodeAllocation{{Node: node, Cores: allocatedCores, CPU: len(allocatedCores), Memory: EffectiveMemory(job.Problem, job.Submission)}})
}

func (s *Scheduler) startJobWithAllocations(clusterName string, job QueuedSubmission, allocations []nodeAllocation) bool {
	if len(allocations) == 0 || allocations[0].Node == nil {
		return false
	}
	primary := allocations[0]
	zap.S().Infof("nodes %s assigned to submission %s", allocationNodeList(allocations), job.Submission.ID)

	var coreStrs []string
	for _, c := range primary.Cores {
		coreStrs = append(coreStrs, strconv.Itoa(c))
	}

	job.Submission.Node = allocationNodeList(allocations)
	job.Submission.Status = models.StatusRunning
	job.Submission.AllocatedCores = strings.Join(coreStrs, ",")
	job.Submission.AllocatedNodeCores = allocationNodeCoreMap(allocations)
	job.Submission.Reason = ""
	if job.Submission.BillingUnits == 0 {
		job.Submission.BillingUnits = EffectiveBilling(s.cfg, job.Problem, job.Submission)
	}

	if err := s.db.Save(job.Submission).Error; err != nil {
		zap.S().Errorf("failed to update submission status for %s: %v", job.Submission.ID, err)
		s.releaseNodeAllocations(clusterName, allocations, job.Submission.ID, true)
		return false
	}
	record := database.AccountingFromSubmission(job.Submission, database.AccountEventStarted)
	record.CPU = schedulingCPUForJob(job)
	record.Memory = EffectiveMemory(job.Problem, job.Submission) * int64(effectiveNodeCount(job))
	if err := database.RecordAccounting(s.db, record); err != nil {
		zap.S().Warnf("failed to record accounting start event for submission %s: %v", job.Submission.ID, err)
	}
	s.notifySubmissionMail(job.Submission, SlurmMailBegin, "")

	go s.dispatcher.Dispatch(job.Submission, job.Problem, primary.Node, primary.Cores)
	return true
}

func (s *Scheduler) updatePendingReason(sub *models.Submission, reason string) {
	if sub == nil || sub.Reason == reason {
		return
	}
	sub.Reason = reason
	if err := s.db.Model(&models.Submission{}).Where("id = ?", sub.ID).Update("reason", reason).Error; err != nil {
		zap.S().Warnf("failed to update pending reason for submission %s: %v", sub.ID, err)
	}
}

func (s *Scheduler) failQueuedSubmission(sub *models.Submission, reason string) {
	if sub == nil {
		return
	}
	zap.S().Warnf("submission %s failed before dispatch: %s", sub.ID, reason)
	sub.Status = models.StatusFailed
	sub.Reason = reason
	sub.Info = models.JSONMap{"error": reason}
	if err := s.db.Save(sub).Error; err != nil {
		zap.S().Errorf("failed to mark submission %s failed: %v", sub.ID, err)
	}
	s.notifySubmissionMail(sub, slurmMailEventForFailure(reason), reason)
}

func (s *Scheduler) findAvailableNode(clusterName string, requiredCPU int, requiredMemory int64) (*NodeState, []int) {
	return s.allocateNode(clusterName, requiredCPU, requiredMemory, "", false, nil)
}

func (s *Scheduler) releaseNodeAllocations(clusterName string, allocations []nodeAllocation, owner string, releaseLicenses bool) {
	for _, allocation := range allocations {
		if allocation.Node == nil {
			continue
		}
		s.releaseResources(clusterName, allocation.Node.Name, allocation.Cores, allocation.Memory, owner, false)
	}
	if releaseLicenses {
		s.ReleaseLicenses(owner)
	}
}

func (s *Scheduler) ReleaseResources(clusterName, nodeName string, coresToRelease []int, memory int64, owner string) {
	s.releaseResources(clusterName, nodeName, coresToRelease, memory, owner, true)
}

func (s *Scheduler) releaseResources(clusterName, nodeName string, coresToRelease []int, memory int64, owner string, releaseLicenses bool) {
	if owner != "" {
		s.reservationMu.Lock()
		delete(s.reservationUsed, owner)
		s.reservationMu.Unlock()
	}
	if cluster, ok := s.clusters[clusterName]; ok {
		for _, name := range splitNodeList(nodeName) {
			node, ok := cluster.Nodes[name]
			if !ok {
				continue
			}
			node.Lock()
			releaseByOwner := owner != "" && (strings.Contains(nodeName, ",") || len(coresToRelease) == 0)
			if releaseByOwner {
				for coreID := range node.UsedCores {
					if coreID < len(node.UsedCoreOwners) && node.UsedCoreOwners[coreID] == owner {
						node.UsedCores[coreID] = false
						node.UsedCoreOwners[coreID] = ""
					}
				}
			} else {
				for _, coreID := range coresToRelease {
					if coreID >= 0 && coreID < len(node.UsedCores) {
						if owner != "" && coreID < len(node.UsedCoreOwners) && node.UsedCoreOwners[coreID] != owner {
							continue
						}
						node.UsedCores[coreID] = false
						if coreID < len(node.UsedCoreOwners) {
							node.UsedCoreOwners[coreID] = ""
						}
					}
				}
			}
			memoryToRelease := memory
			if owner != "" {
				memoryToRelease = node.MemoryAllocations[owner]
				delete(node.MemoryAllocations, owner)
			}
			if owner != "" && node.ExclusiveOwner == owner {
				node.ExclusiveOwner = ""
			}
			node.UsedMemory -= memoryToRelease
			if node.UsedMemory < 0 {
				node.UsedMemory = 0
			}
			node.Unlock()
			var coreStrs []string
			for _, c := range coresToRelease {
				coreStrs = append(coreStrs, strconv.Itoa(c))
			}
			zap.S().Infof("released resources (cores: [%s], mem: %dMB) from node %s", strings.Join(coreStrs, ","), memoryToRelease, name)
		}
	}
	if releaseLicenses {
		s.ReleaseLicenses(owner)
	}
}

func splitNodeList(nodeList string) []string {
	parts := strings.Split(nodeList, ",")
	nodes := make([]string, 0, len(parts))
	for _, part := range parts {
		if name := strings.TrimSpace(part); name != "" {
			nodes = append(nodes, name)
		}
	}
	return nodes
}

func allocationNodeList(allocations []nodeAllocation) string {
	names := make([]string, 0, len(allocations))
	for _, allocation := range allocations {
		if allocation.Node != nil {
			names = append(names, allocation.Node.Name)
		}
	}
	return strings.Join(names, ",")
}

func allocationNodeCoreMap(allocations []nodeAllocation) string {
	parts := make([]string, 0, len(allocations))
	for _, allocation := range allocations {
		if allocation.Node == nil {
			continue
		}
		coreStrs := make([]string, 0, len(allocation.Cores))
		for _, coreID := range allocation.Cores {
			coreStrs = append(coreStrs, strconv.Itoa(coreID))
		}
		parts = append(parts, allocation.Node.Name+":"+strings.Join(coreStrs, ","))
	}
	return strings.Join(parts, ";")
}

func copyMemoryAllocations(source map[string]int64) map[string]int64 {
	if source == nil {
		return nil
	}
	out := make(map[string]int64, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}
