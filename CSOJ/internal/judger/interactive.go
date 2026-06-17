package judger

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	defaultInteractiveImage          = "alpine:latest"
	defaultInteractiveTimeoutSeconds = 3600
)

type InteractiveAllocationRequest struct {
	UserID       string `json:"user_id"`
	Cluster      string `json:"partition"`
	CPU          int    `json:"cpus"`
	Memory       int64  `json:"memory"`
	Nodes        int    `json:"nodes"`
	Account      string `json:"account"`
	QOS          string `json:"qos"`
	TRES         string `json:"tres"`
	GRES         string `json:"gres"`
	TimeLimit    int    `json:"time_limit"`
	Constraint   string `json:"constraint"`
	Reservation  string `json:"reservation"`
	NodeList     string `json:"nodelist"`
	ExcludeNodes string `json:"exclude_nodes"`
	Exclusive    bool   `json:"exclusive"`
}

type InteractiveRunRequest struct {
	AllocationID string   `json:"allocation_id"`
	Command      []string `json:"command"`
	CommandLine  string   `json:"command_line"`
	Image        string   `json:"image"`
	Timeout      int      `json:"timeout"`
	CPU          int      `json:"cpus"`
	Memory       int64    `json:"memory"`
	Root         bool     `json:"root"`
	Network      bool     `json:"network"`
}

func (s *Scheduler) AllocateInteractive(req InteractiveAllocationRequest) (*models.Allocation, error) {
	if req.CPU <= 0 {
		return nil, fmt.Errorf("cpus must be positive")
	}
	if req.Memory < 0 {
		return nil, fmt.Errorf("memory must be non-negative")
	}
	if req.Nodes < 0 {
		return nil, fmt.Errorf("nodes must be non-negative")
	}
	clusterName := req.Cluster
	if clusterName == "" {
		clusterName = s.defaultClusterName()
	}
	if clusterName == "" {
		return nil, fmt.Errorf("no scheduler partition configured")
	}
	if _, ok := s.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("partition %q not found", clusterName)
	}

	allocationID := "alloc-" + uuid.NewString()
	sub := &models.Submission{
		ID:           allocationID,
		UserID:       req.UserID,
		Status:       models.StatusQueued,
		Cluster:      clusterName,
		Account:      req.Account,
		QOS:          req.QOS,
		TRES:         req.TRES,
		GRES:         req.GRES,
		Nodes:        req.Nodes,
		TimeLimit:    req.TimeLimit,
		Constraint:   req.Constraint,
		Reservation:  req.Reservation,
		NodeList:     req.NodeList,
		ExcludeNodes: req.ExcludeNodes,
		Exclusive:    req.Exclusive,
		CreatedAt:    time.Now(),
	}
	problem := &Problem{
		ID:      "interactive",
		Cluster: clusterName,
		CPU:     req.CPU,
		Memory:  req.Memory,
		Scheduling: SchedulingConfig{
			Account:      req.Account,
			QOS:          req.QOS,
			TRES:         req.TRES,
			GRES:         req.GRES,
			TimeLimit:    req.TimeLimit,
			Constraint:   req.Constraint,
			Reservation:  req.Reservation,
			NodeList:     req.NodeList,
			ExcludeNodes: req.ExcludeNodes,
		},
	}
	job := QueuedSubmission{Submission: sub, Problem: problem}
	if decision, reason := s.evaluateJob(clusterName, job, time.Now()); decision != jobDecisionRun {
		if reason == "" {
			reason = "Rejected"
		}
		return nil, fmt.Errorf("%s", reason)
	}

	if !s.AcquireLicensesForJob(job, allocationID) {
		return nil, fmt.Errorf("licenses unavailable")
	}
	licenseAcquired := true
	defer func() {
		if licenseAcquired {
			s.ReleaseLicenses(allocationID)
		}
	}()

	allocations := s.findAvailableNodeAllocationsForJob(clusterName, job, time.Now())
	if len(allocations) == 0 {
		return nil, fmt.Errorf("resources unavailable")
	}

	primary := allocations[0]
	coreStrs := make([]string, 0, len(primary.Cores))
	for _, coreID := range primary.Cores {
		coreStrs = append(coreStrs, strconv.Itoa(coreID))
	}

	allocation := &models.Allocation{
		ID:                 allocationID,
		Status:             models.AllocationActive,
		UserID:             req.UserID,
		Cluster:            clusterName,
		Node:               allocationNodeList(allocations),
		CPU:                schedulingCPUForJob(job),
		Memory:             req.Memory,
		Nodes:              effectiveNodeCount(job),
		AllocatedCores:     strings.Join(coreStrs, ","),
		AllocatedNodeCores: allocationNodeCoreMap(allocations),
		Account:            req.Account,
		QOS:                req.QOS,
		TRES:               req.TRES,
		GRES:               req.GRES,
		BillingUnits:       CalculateBilling(s.cfg, problem, sub),
		TimeLimit:          req.TimeLimit,
		Constraint:         req.Constraint,
		Reservation:        req.Reservation,
		NodeList:           req.NodeList,
		ExcludeNodes:       req.ExcludeNodes,
		Exclusive:          req.Exclusive,
	}
	if err := s.db.Create(allocation).Error; err != nil {
		s.releaseNodeAllocations(clusterName, allocations, allocationID, false)
		return nil, err
	}
	licenseAcquired = false

	if err := database.RecordAccounting(s.db, accountingFromAllocation(allocation, database.AccountEventAllocated)); err != nil {
		zap.S().Warnf("failed to record allocation accounting event for %s: %v", allocation.ID, err)
	}
	return allocation, nil
}

func (s *Scheduler) ReleaseInteractiveAllocation(id string) (*models.Allocation, error) {
	return s.releaseInteractiveAllocation(id, "Released")
}

func (s *Scheduler) releaseInteractiveAllocation(id, reason string) (*models.Allocation, error) {
	if strings.TrimSpace(reason) == "" {
		reason = "Released"
	}
	var allocation models.Allocation
	err := s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).First(&allocation).Error; err != nil {
			return err
		}
		if allocation.Status != models.AllocationActive {
			return fmt.Errorf("allocation %s is not active", id)
		}
		now := time.Now()
		if err := tx.Model(&models.Allocation{}).Where("id = ?", id).Updates(map[string]interface{}{
			"status":      models.AllocationReleased,
			"released_at": &now,
			"reason":      reason,
		}).Error; err != nil {
			return err
		}
		allocation.Status = models.AllocationReleased
		allocation.ReleasedAt = &now
		allocation.Reason = reason
		return nil
	})
	if err != nil {
		return nil, err
	}

	s.ReleaseResources(allocation.Cluster, allocation.Node, parseAllocatedCores(allocation.AllocatedCores), allocation.Memory, allocation.ID)
	s.cleanupInteractiveAllocationRuntime(&allocation)
	if err := database.RecordAccounting(s.db, accountingFromAllocation(&allocation, database.AccountEventAllocationReleased)); err != nil {
		zap.S().Warnf("failed to record allocation release accounting event for %s: %v", allocation.ID, err)
	}
	return &allocation, nil
}

func (s *Scheduler) GetInteractiveAllocation(id string) (*models.Allocation, error) {
	var allocation models.Allocation
	if err := s.db.Where("id = ?", id).First(&allocation).Error; err != nil {
		return nil, err
	}
	return &allocation, nil
}

func (s *Scheduler) ListInteractiveAllocations(status string) ([]models.Allocation, error) {
	query := s.db.Model(&models.Allocation{}).Order("created_at desc")
	if status != "" {
		query = query.Where("status = ?", status)
	}
	var allocations []models.Allocation
	if err := query.Find(&allocations).Error; err != nil {
		return nil, err
	}
	return allocations, nil
}

func (s *Scheduler) RunInteractiveStep(req InteractiveRunRequest) (*models.RunStep, error) {
	if strings.TrimSpace(req.AllocationID) == "" {
		return nil, fmt.Errorf("allocation_id is required")
	}
	command, commandText, err := normalizeInteractiveCommand(req)
	if err != nil {
		return nil, err
	}

	allocation, err := s.GetInteractiveAllocation(req.AllocationID)
	if err != nil {
		return nil, err
	}
	if allocation.Status != models.AllocationActive {
		return nil, fmt.Errorf("allocation %s is not active", allocation.ID)
	}

	nodeConfig, ok := FindNodeConfig(s.cfg, allocation.Cluster, allocation.Node)
	if !ok {
		return nil, fmt.Errorf("node config %q/%q not found", allocation.Cluster, allocation.Node)
	}
	factory := s.runtimeFactory
	if factory == nil {
		factory = NewRuntimeManager
	}
	runtime, err := factory(nodeConfig)
	if err != nil {
		return nil, err
	}

	image := strings.TrimSpace(req.Image)
	if image == "" {
		image = defaultInteractiveImage
	}
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = allocation.TimeLimit
	}
	if timeout <= 0 {
		timeout = defaultInteractiveTimeoutSeconds
	}
	timeout, err = s.interactiveStepTimeout(allocation, timeout)
	if err != nil {
		return nil, err
	}
	stepCPU, stepMemory, err := normalizeInteractiveStepResources(req, allocation)
	if err != nil {
		return nil, err
	}

	step := &models.RunStep{
		ID:           "step-" + uuid.NewString(),
		AllocationID: allocation.ID,
		UserID:       allocation.UserID,
		Cluster:      allocation.Cluster,
		Node:         allocation.Node,
		Image:        image,
		Runtime:      NodeRuntimeName(nodeConfig),
		Command:      commandText,
		Status:       models.StatusRunning,
		Timeout:      timeout,
		CPU:          stepCPU,
		Memory:       stepMemory,
		StartedAt:    time.Now(),
	}
	if err := s.createInteractiveRunStep(allocation, step); err != nil {
		return nil, err
	}
	if err := database.RecordAccounting(s.db, accountingFromRunStep(step, allocation, database.AccountEventRunStarted)); err != nil {
		zap.S().Warnf("failed to record srun start event for %s: %v", step.ID, err)
	}

	failBeforeExec := func(reason string, exitCode int, err error) (*models.RunStep, error) {
		s.finishInteractiveStep(step, allocation, models.StatusFailed, exitCode, "", "", RuntimeUsage{}, reason, database.AccountEventRunFailed)
		return step, err
	}

	if err := runtime.CreateVolume(allocation.ID); err != nil {
		return failBeforeExec("RuntimeError", -1, fmt.Errorf("failed to create allocation work volume: %w", err))
	}

	envs := interactiveRunEnv(allocation, step)
	containerID, err := runtime.CreateContainer(image, allocation.ID, step.CPU, step.AllocatedCores, step.Memory, req.Root, nil, req.Network, step.ID, envs)
	if err != nil {
		return failBeforeExec("RuntimeError", -1, fmt.Errorf("failed to create srun container: %w", err))
	}
	step.ContainerID = containerID
	if err := s.db.Model(&models.RunStep{}).Where("id = ?", step.ID).Update("container_id", containerID).Error; err != nil {
		zap.S().Warnf("failed to update srun container id for %s: %v", step.ID, err)
	}
	defer runtime.CleanupContainer(containerID)

	if err := runtime.StartContainer(containerID); err != nil {
		return failBeforeExec("RuntimeError", -1, fmt.Errorf("failed to start srun container: %w", err))
	}

	broadcastDir := InteractiveBroadcastDir(s.cfg.Storage.SubmissionContent, allocation.ID)
	if info, err := os.Stat(broadcastDir); err == nil && info.IsDir() {
		if err := runtime.CopyToContainer(containerID, broadcastDir, "/"); err != nil {
			return failBeforeExec("RuntimeError", -1, fmt.Errorf("failed to copy sbcast files to container: %w", err))
		}
	} else if err != nil && !os.IsNotExist(err) {
		return failBeforeExec("RuntimeError", -1, fmt.Errorf("failed to inspect sbcast files: %w", err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	result, execErr := runtime.ExecInContainer(ctx, containerID, command, func(string, []byte) {})

	status := models.StatusSuccess
	reason := ""
	event := database.AccountEventRunCompleted
	if execErr != nil || result.ExitCode != 0 {
		status = models.StatusFailed
		event = database.AccountEventRunFailed
		switch {
		case ctx.Err() != nil:
			reason = "TimeLimit"
			result.ExitCode = -1
		case execErr != nil:
			reason = "RuntimeError"
			if result.ExitCode == 0 {
				result.ExitCode = -1
			}
		default:
			reason = "NonZeroExitCode"
		}
	}
	s.finishInteractiveStep(step, allocation, status, result.ExitCode, result.Stdout, result.Stderr, result.Usage, reason, event)
	return step, nil
}

func (s *Scheduler) GetInteractiveRunStep(id string) (*models.RunStep, error) {
	var step models.RunStep
	if err := s.db.Where("id = ?", id).First(&step).Error; err != nil {
		return nil, err
	}
	return &step, nil
}

func (s *Scheduler) ListInteractiveRunSteps(allocationID, status string) ([]models.RunStep, error) {
	query := s.db.Model(&models.RunStep{}).Order("created_at desc")
	if strings.TrimSpace(allocationID) != "" {
		query = query.Where("allocation_id = ?", allocationID)
	}
	if strings.TrimSpace(status) != "" {
		query = query.Where("status = ?", status)
	}
	var steps []models.RunStep
	if err := query.Find(&steps).Error; err != nil {
		return nil, err
	}
	return steps, nil
}

func (s *Scheduler) defaultClusterName() string {
	names := make([]string, 0, len(s.clusters))
	for name := range s.clusters {
		names = append(names, name)
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	return names[0]
}

func normalizeInteractiveCommand(req InteractiveRunRequest) ([]string, string, error) {
	if len(req.Command) > 0 {
		command := make([]string, 0, len(req.Command))
		for _, part := range req.Command {
			if strings.TrimSpace(part) != "" {
				command = append(command, part)
			}
		}
		if len(command) == 0 {
			return nil, "", fmt.Errorf("command must not be empty")
		}
		data, err := json.Marshal(command)
		if err != nil {
			return nil, "", err
		}
		return command, string(data), nil
	}
	commandLine := strings.TrimSpace(req.CommandLine)
	if commandLine == "" {
		return nil, "", fmt.Errorf("command or command_line is required")
	}
	return []string{"/bin/sh", "-lc", commandLine}, commandLine, nil
}

func normalizeInteractiveStepResources(req InteractiveRunRequest, allocation *models.Allocation) (int, int64, error) {
	if req.CPU < 0 {
		return 0, 0, fmt.Errorf("cpus must be non-negative")
	}
	if req.Memory < 0 {
		return 0, 0, fmt.Errorf("memory must be non-negative")
	}
	cpu := req.CPU
	if cpu == 0 {
		cpu = interactivePrimaryCPU(allocation)
	}
	if cpu <= 0 {
		return 0, 0, fmt.Errorf("cpus must be positive")
	}
	if cpu > interactivePrimaryCPU(allocation) {
		return 0, 0, fmt.Errorf("step cpus exceed allocation cpus")
	}
	memory := req.Memory
	if memory == 0 {
		memory = allocation.Memory
	}
	if allocation.Memory > 0 && memory > allocation.Memory {
		return 0, 0, fmt.Errorf("step memory exceeds allocation memory")
	}
	return cpu, memory, nil
}

func (s *Scheduler) interactiveStepTimeout(allocation *models.Allocation, requestedTimeout int) (int, error) {
	if allocation == nil || allocation.TimeLimit <= 0 {
		return requestedTimeout, nil
	}
	deadline := allocation.CreatedAt.Add(time.Duration(allocation.TimeLimit) * time.Second)
	remaining := time.Until(deadline)
	if remaining <= 0 {
		if _, err := s.releaseInteractiveAllocation(allocation.ID, "TimeLimit"); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("allocation %s exceeded time limit", allocation.ID)
	}
	remainingSeconds := int(math.Ceil(remaining.Seconds()))
	if requestedTimeout <= 0 || remainingSeconds < requestedTimeout {
		return remainingSeconds, nil
	}
	return requestedTimeout, nil
}

func (s *Scheduler) createInteractiveRunStep(allocation *models.Allocation, step *models.RunStep) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		var locked models.Allocation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", allocation.ID).First(&locked).Error; err != nil {
			return err
		}
		if locked.Status != models.AllocationActive {
			return fmt.Errorf("allocation %s is not active", allocation.ID)
		}
		var runningSteps []models.RunStep
		if err := tx.Where("allocation_id = ? AND status = ?", allocation.ID, models.StatusRunning).Find(&runningSteps).Error; err != nil {
			return err
		}
		if err := validateInteractiveStepResources(locked, runningSteps, step); err != nil {
			return err
		}
		cores, err := assignInteractiveStepCores(locked.AllocatedCores, runningSteps, step.CPU)
		if err != nil {
			return err
		}

		*allocation = locked
		step.AllocationID = locked.ID
		step.UserID = locked.UserID
		step.Cluster = locked.Cluster
		step.Node = locked.Node
		step.AllocatedCores = cores
		return tx.Create(step).Error
	})
}

func validateInteractiveStepResources(allocation models.Allocation, runningSteps []models.RunStep, step *models.RunStep) error {
	usedCPU := 0
	var usedMemory int64
	for _, running := range runningSteps {
		cpu := running.CPU
		if cpu <= 0 {
			cpu = allocation.CPU
		}
		usedCPU += cpu
		memory := running.Memory
		if memory == 0 {
			memory = allocation.Memory
		}
		usedMemory += memory
	}
	if usedCPU+step.CPU > interactivePrimaryCPU(&allocation) {
		return fmt.Errorf("step CPU resources unavailable")
	}
	if allocation.Memory > 0 && usedMemory+step.Memory > allocation.Memory {
		return fmt.Errorf("step memory resources unavailable")
	}
	return nil
}

func assignInteractiveStepCores(allocatedCores string, runningSteps []models.RunStep, cpu int) (string, error) {
	cores := parseAllocatedCores(allocatedCores)
	if len(cores) == 0 {
		return allocatedCores, nil
	}
	used := make(map[int]bool)
	for _, running := range runningSteps {
		for _, coreID := range parseAllocatedCores(running.AllocatedCores) {
			used[coreID] = true
		}
	}
	selected := make([]string, 0, cpu)
	for _, coreID := range cores {
		if used[coreID] {
			continue
		}
		selected = append(selected, strconv.Itoa(coreID))
		if len(selected) == cpu {
			return strings.Join(selected, ","), nil
		}
	}
	return "", fmt.Errorf("step CPU resources unavailable")
}

func interactivePrimaryCPU(allocation *models.Allocation) int {
	if allocation == nil {
		return 0
	}
	if cores := parseAllocatedCores(allocation.AllocatedCores); len(cores) > 0 {
		return len(cores)
	}
	return allocation.CPU
}

func interactiveAllocationNodeCount(allocation *models.Allocation) int {
	if allocation == nil {
		return 1
	}
	if allocation.Nodes > 0 {
		return allocation.Nodes
	}
	if nodes := splitNodeList(allocation.Node); len(nodes) > 0 {
		return len(nodes)
	}
	return 1
}

func interactiveAllocationCPUsPerNode(allocation *models.Allocation) string {
	if allocation == nil {
		return ""
	}
	if strings.TrimSpace(allocation.AllocatedNodeCores) == "" {
		return strconv.Itoa(interactivePrimaryCPU(allocation))
	}
	parts := strings.Split(allocation.AllocatedNodeCores, ";")
	counts := make([]string, 0, len(parts))
	for _, part := range parts {
		_, cores, ok := strings.Cut(part, ":")
		if !ok {
			continue
		}
		counts = append(counts, strconv.Itoa(len(parseAllocatedCores(cores))))
	}
	if len(counts) == 0 {
		return strconv.Itoa(interactivePrimaryCPU(allocation))
	}
	return strings.Join(counts, ",")
}

func (s *Scheduler) finishInteractiveStep(step *models.RunStep, allocation *models.Allocation, status models.Status, exitCode int, stdout, stderr string, usage RuntimeUsage, reason, event string) {
	now := time.Now()
	updates := map[string]interface{}{
		"status":      status,
		"exit_code":   exitCode,
		"stdout":      stdout,
		"stderr":      stderr,
		"reason":      reason,
		"ave_cpu":     usage.AveCPU,
		"ave_rss":     usage.AveRSS,
		"max_rss":     usage.MaxRSS,
		"max_vm_size": usage.MaxVMSize,
		"finished_at": now,
	}
	if err := s.db.Model(&models.RunStep{}).Where("id = ?", step.ID).Updates(updates).Error; err != nil {
		zap.S().Warnf("failed to finish srun step %s: %v", step.ID, err)
	}
	step.Status = status
	step.ExitCode = exitCode
	step.Stdout = stdout
	step.Stderr = stderr
	step.Reason = reason
	step.AveCPU = usage.AveCPU
	step.AveRSS = usage.AveRSS
	step.MaxRSS = usage.MaxRSS
	step.MaxVMSize = usage.MaxVMSize
	step.FinishedAt = now

	if err := database.RecordAccounting(s.db, accountingFromRunStep(step, allocation, event)); err != nil {
		zap.S().Warnf("failed to record srun finish event for %s: %v", step.ID, err)
	}
}

func (s *Scheduler) cleanupInteractiveAllocationRuntime(allocation *models.Allocation) {
	if allocation == nil {
		return
	}
	var stepCount int64
	if err := s.db.Model(&models.RunStep{}).Where("allocation_id = ?", allocation.ID).Count(&stepCount).Error; err != nil {
		zap.S().Warnf("failed to count srun steps for allocation %s: %v", allocation.ID, err)
		return
	}
	if stepCount == 0 {
		return
	}
	nodeConfig, ok := FindNodeConfig(s.cfg, allocation.Cluster, allocation.Node)
	if !ok {
		zap.S().Warnf("node config %q/%q not found while releasing allocation %s", allocation.Cluster, allocation.Node, allocation.ID)
		return
	}
	factory := s.runtimeFactory
	if factory == nil {
		factory = NewRuntimeManager
	}
	runtime, err := factory(nodeConfig)
	if err != nil {
		zap.S().Warnf("failed to create runtime while releasing allocation %s: %v", allocation.ID, err)
		return
	}
	if err := runtime.RemoveVolume(allocation.ID); err != nil {
		zap.S().Warnf("failed to remove allocation work volume %s: %v", allocation.ID, err)
	}
	if err := os.RemoveAll(InteractiveBroadcastDir(s.cfg.Storage.SubmissionContent, allocation.ID)); err != nil {
		zap.S().Warnf("failed to remove sbcast staging directory for allocation %s: %v", allocation.ID, err)
	}
}

func interactiveRunEnv(allocation *models.Allocation, step *models.RunStep) []string {
	return []string{
		"CSOJ_ALLOCATION_ID=" + allocation.ID,
		"CSOJ_RUN_STEP_ID=" + step.ID,
		"SLURM_JOB_ID=" + allocation.ID,
		"SLURM_STEP_ID=" + step.ID,
		"SLURM_JOB_PARTITION=" + allocation.Cluster,
		"SLURM_JOB_NODELIST=" + allocation.Node,
		"SLURM_JOB_NUM_NODES=" + strconv.Itoa(interactiveAllocationNodeCount(allocation)),
		"SLURM_NNODES=" + strconv.Itoa(interactiveAllocationNodeCount(allocation)),
		"SLURM_CPUS_ON_NODE=" + strconv.Itoa(interactivePrimaryCPU(allocation)),
		"SLURM_JOB_CPUS_PER_NODE=" + interactiveAllocationCPUsPerNode(allocation),
		"SLURM_CPUS_PER_TASK=" + strconv.Itoa(step.CPU),
		"SLURM_STEP_CPUS=" + strconv.Itoa(step.CPU),
		"SLURM_STEP_MEMORY=" + strconv.FormatInt(step.Memory, 10),
		"SLURM_TRES_PER_JOB=" + allocation.TRES,
	}
}

func accountingFromAllocation(allocation *models.Allocation, event string) models.AccountingRecord {
	if allocation == nil {
		return models.AccountingRecord{Event: event}
	}
	return models.AccountingRecord{
		SubmissionID: allocation.ID,
		UserID:       allocation.UserID,
		Cluster:      allocation.Cluster,
		Node:         allocation.Node,
		Account:      allocation.Account,
		QOS:          allocation.QOS,
		Event:        event,
		State:        models.StatusRunning,
		CPU:          allocation.CPU,
		Memory:       allocation.Memory * int64(interactiveAllocationNodeCount(allocation)),
		TRES:         allocation.TRES,
		BillingUnits: allocation.BillingUnits,
		Reason:       allocation.Reason,
		Message:      "interactive allocation",
	}
}

func accountingFromRunStep(step *models.RunStep, allocation *models.Allocation, event string) models.AccountingRecord {
	if step == nil {
		return models.AccountingRecord{Event: event}
	}
	record := models.AccountingRecord{
		SubmissionID: step.AllocationID,
		ContainerID:  step.ContainerID,
		UserID:       step.UserID,
		Cluster:      step.Cluster,
		Node:         step.Node,
		Event:        event,
		State:        step.Status,
		StepName:     step.ID,
		ExitCode:     step.ExitCode,
		CPU:          step.CPU,
		Memory:       step.Memory,
		Reason:       step.Reason,
		Message:      step.Command,
	}
	if allocation != nil {
		record.Account = allocation.Account
		record.QOS = allocation.QOS
		if record.CPU == 0 {
			record.CPU = allocation.CPU
		}
		if record.Memory == 0 {
			record.Memory = allocation.Memory
		}
		record.TRES = allocation.TRES
		record.BillingUnits = allocation.BillingUnits
	}
	return record
}
