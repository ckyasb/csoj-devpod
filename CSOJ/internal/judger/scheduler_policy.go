package judger

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
)

type jobDecision int

const (
	jobDecisionRun jobDecision = iota
	jobDecisionWait
	jobDecisionFail
)

type nodeFilter func(*NodeState) bool

type PriorityBreakdown struct {
	JobID             string        `json:"job_id"`
	ArrayJobID        string        `json:"array_job_id"`
	ArrayTaskID       int           `json:"array_task_id"`
	UserID            string        `json:"user_id"`
	Partition         string        `json:"partition"`
	Account           string        `json:"account"`
	QOS               string        `json:"qos"`
	Status            models.Status `json:"status"`
	SlurmState        string        `json:"slurm_state"`
	Priority          int           `json:"priority"`
	ManualPriority    int           `json:"manual_priority"`
	PartitionPriority int           `json:"partition_priority"`
	QOSPriority       int           `json:"qos_priority"`
	FairsharePriority int           `json:"fairshare_priority"`
	FairsharePenalty  int           `json:"fairshare_penalty"`
	AgePriority       int           `json:"age_priority"`
	JobSizePriority   int           `json:"job_size_priority"`
	NicePenalty       int           `json:"nice_penalty"`
}

type FairshareRecord struct {
	Account          string  `json:"account"`
	ParentAccount    string  `json:"parent_account"`
	RawShares        int     `json:"raw_shares"`
	NormalizedShares float64 `json:"normalized_shares"`
	RawUsage         float64 `json:"raw_usage"`
	EffectiveUsage   float64 `json:"effective_usage"`
	UsagePenalty     int     `json:"usage_penalty"`
	RunningJobs      int64   `json:"running_jobs"`
	SubmittedJobs    int64   `json:"submitted_jobs"`
}

func (s *Scheduler) backfillEnabled() bool {
	if s.cfg.Scheduler.Backfill == nil {
		return true
	}
	return *s.cfg.Scheduler.Backfill
}

func defaultPriorityWeights(weights config.PriorityWeights) config.PriorityWeights {
	if weights.Age == 0 && weights.QOS == 0 && weights.Nice == 0 &&
		weights.Partition == 0 && weights.JobSize == 0 && weights.Fairshare == 0 {
		return config.PriorityWeights{
			Age:       1,
			QOS:       1000,
			Nice:      1,
			Partition: 10000,
			JobSize:   1,
			Fairshare: 10,
		}
	}
	return weights
}

func (s *Scheduler) jobPriority(clusterName string, job QueuedSubmission, now time.Time) int {
	return s.priorityBreakdown(clusterName, job, now).Priority
}

func (s *Scheduler) GetPriorityBreakdowns(status string) ([]PriorityBreakdown, error) {
	query := s.db.Model(&models.Submission{}).Order("created_at asc")
	if strings.TrimSpace(status) != "" {
		query = query.Where("status = ?", status)
	} else {
		query = query.Where("status = ?", models.StatusQueued)
	}
	var submissions []models.Submission
	if err := query.Find(&submissions).Error; err != nil {
		return nil, err
	}

	now := time.Now()
	out := make([]PriorityBreakdown, 0, len(submissions))
	s.appState.RLock()
	defer s.appState.RUnlock()
	for i := range submissions {
		sub := submissions[i]
		problem, ok := s.appState.Problems[sub.ProblemID]
		if !ok {
			continue
		}
		clusterName := sub.Cluster
		if clusterName == "" {
			clusterName = problem.Cluster
		}
		if clusterName == "" {
			clusterName = s.defaultClusterName()
		}
		out = append(out, s.priorityBreakdown(clusterName, QueuedSubmission{Submission: &sub, Problem: problem}, now))
	}
	return out, nil
}

func (s *Scheduler) GetFairshareRecords() []FairshareRecord {
	accounts := s.ListAccounts("")
	totalShares := 0
	for _, account := range accounts {
		if account.Fairshare > 0 {
			totalShares += account.Fairshare
		}
	}
	if totalShares <= 0 {
		totalShares = len(accounts)
	}
	now := time.Now()
	records := make([]FairshareRecord, 0, len(accounts))
	for _, account := range accounts {
		shares := account.Fairshare
		if shares <= 0 {
			shares = 1
		}
		rawUsage := s.decayedAccountBillingUsage(account.Name, now)
		records = append(records, FairshareRecord{
			Account:          account.Name,
			ParentAccount:    account.ParentName,
			RawShares:        shares,
			NormalizedShares: float64(shares) / float64(totalShares),
			RawUsage:         rawUsage,
			EffectiveUsage:   rawUsage / float64(shares),
			UsagePenalty:     s.fairshareUsagePenalty(account.Name, now),
			RunningJobs:      s.countSubmissions("account = ? AND status IN ?", account.Name, activeStatuses()),
			SubmittedJobs:    s.countSubmissions("account = ? AND status IN ?", account.Name, submitStatuses()),
		})
	}
	return records
}

func (s *Scheduler) priorityBreakdown(clusterName string, job QueuedSubmission, now time.Time) PriorityBreakdown {
	weights := defaultPriorityWeights(s.cfg.Scheduler.PriorityWeights)

	priority := job.Submission.Priority
	if priority == 0 {
		priority = job.Problem.Scheduling.Priority
	}
	breakdown := PriorityBreakdown{
		JobID:          job.Submission.ID,
		ArrayJobID:     job.Submission.ArrayJobID,
		ArrayTaskID:    job.Submission.ArrayTaskID,
		UserID:         job.Submission.UserID,
		Partition:      clusterName,
		Account:        effectiveAccount(job),
		QOS:            effectiveQOS(job),
		Status:         job.Submission.Status,
		ManualPriority: priority,
	}
	breakdown.SlurmState, _ = models.DeriveSlurmJobState(job.Submission.Status, job.Submission.Hold, job.Submission.Reason)

	if cluster, ok := s.clusters[clusterName]; ok && cluster.Cluster != nil {
		breakdown.PartitionPriority = cluster.PriorityTier * weights.Partition
		priority += breakdown.PartitionPriority
	}

	if qosName := effectiveQOS(job); qosName != "" {
		if qos, ok := s.qosByName(qosName); ok {
			breakdown.QOSPriority = qos.Priority * weights.QOS
			priority += breakdown.QOSPriority
		}
	}
	if accountName := effectiveAccount(job); accountName != "" {
		if account, ok := s.accountByName(accountName); ok {
			breakdown.FairsharePriority = account.Fairshare * weights.Fairshare
			priority += breakdown.FairsharePriority
		}
		breakdown.FairsharePenalty = s.fairshareUsagePenalty(accountName, now)
		priority -= breakdown.FairsharePenalty
	}

	ageMinutes := int(now.Sub(job.Submission.CreatedAt).Minutes())
	if ageMinutes > 0 {
		breakdown.AgePriority = ageMinutes * weights.Age
		priority += breakdown.AgePriority
	}

	breakdown.JobSizePriority = EffectiveCPU(job.Problem, job.Submission) * weights.JobSize
	priority += breakdown.JobSizePriority
	breakdown.NicePenalty = effectiveNice(job) * weights.Nice
	priority -= breakdown.NicePenalty
	breakdown.Priority = priority
	return breakdown
}

func (s *Scheduler) evaluateJob(clusterName string, job QueuedSubmission, now time.Time) (jobDecision, string) {
	if job.Submission == nil || job.Problem == nil {
		return jobDecisionFail, "InvalidJob"
	}

	cluster, ok := s.clusters[clusterName]
	if !ok {
		return jobDecisionFail, "InvalidPartition"
	}
	if !clusterStateSchedulable(cluster.State) {
		return jobDecisionWait, "PartitionDown"
	}

	sub := job.Submission
	if sub.Hold {
		return jobDecisionWait, "JobHeld"
	}
	if sub.BeginTime != nil && now.Before(*sub.BeginTime) {
		return jobDecisionWait, "BeginTime"
	}
	if sub.Deadline != nil && now.After(*sub.Deadline) {
		return jobDecisionFail, "Deadline"
	}

	user, username := s.lookupUser(sub.UserID)
	account := effectiveAccount(job)
	qos := effectiveQOS(job)
	if reason := s.validateAssociations(cluster.Cluster, user, username, account, qos); reason != "" {
		return jobDecisionFail, reason
	}
	if reason := s.validateLimits(clusterName, cluster.Cluster, job, qos); reason != "" {
		return jobDecisionWait, reason
	}
	if reason := s.validateArrayLimits(sub); reason != "" {
		return jobDecisionWait, reason
	}

	if decision, reason := s.evaluateDependencies(sub); decision != jobDecisionRun {
		return decision, reason
	}
	if decision, reason := s.evaluateReservation(clusterName, job, username, account, now); decision != jobDecisionRun {
		return decision, reason
	}

	return jobDecisionRun, ""
}

func (s *Scheduler) validateArrayLimits(sub *models.Submission) string {
	if sub.ArrayJobID == "" || sub.ArrayMaxRunning <= 0 {
		return ""
	}
	running := s.countSubmissions(
		"array_job_id = ? AND status IN ?",
		sub.ArrayJobID,
		activeStatuses(),
	)
	if running >= int64(sub.ArrayMaxRunning) {
		return "ArrayTaskLimit"
	}
	return ""
}

func (s *Scheduler) validateAssociations(cluster *config.Cluster, user *models.User, username, account, qos string) string {
	if cluster == nil {
		return "InvalidPartition"
	}
	if len(cluster.AllowUsers) > 0 && !stringAllowed(username, cluster.AllowUsers) && !stringAllowed(user.ID, cluster.AllowUsers) {
		return "PartitionUserLimit"
	}
	if account != "" && len(cluster.AllowAccounts) > 0 && !stringAllowed(account, cluster.AllowAccounts) {
		return "PartitionAccountLimit"
	}
	if qos != "" && len(cluster.AllowQOS) > 0 && !stringAllowed(qos, cluster.AllowQOS) {
		return "PartitionQOSLimit"
	}
	if qos != "" && stringAllowed(qos, cluster.DenyQOS) {
		return "PartitionQOSDenied"
	}

	accountCfg, accountKnown := s.accountByName(account)
	if account != "" && s.hasAccountsConfigured() && !accountKnown {
		return "InvalidAccount"
	}
	if accountKnown {
		if len(accountCfg.Users) > 0 && !stringAllowed(username, accountCfg.Users) && !stringAllowed(user.ID, accountCfg.Users) {
			return "InvalidAccount"
		}
		if qos != "" && len(accountCfg.AllowQOS) > 0 && !stringAllowed(qos, accountCfg.AllowQOS) {
			return "InvalidQOS"
		}
	}
	if qos != "" && s.hasQOSConfigured() {
		if _, ok := s.qosByName(qos); !ok {
			return "InvalidQOS"
		}
	}
	return ""
}

func (s *Scheduler) validateLimits(clusterName string, cluster *config.Cluster, job QueuedSubmission, qosName string) string {
	sub := job.Submission
	timeLimit := effectiveTimeLimit(job)
	billingUnits := EffectiveBilling(s.cfg, job.Problem, sub)
	if cluster != nil {
		if cluster.MaxTime > 0 && timeLimit > cluster.MaxTime {
			return "PartitionTimeLimit"
		}
		if cluster.MaxJobs > 0 && s.countActiveJobsForCluster(clusterName) >= int64(cluster.MaxJobs) {
			return "PartitionJobLimit"
		}
	}
	if accountName := effectiveAccount(job); accountName != "" {
		if account, ok := s.accountByName(accountName); ok {
			if account.MaxJobs > 0 {
				running := s.countActiveJobsForAccount(accountName)
				if running >= int64(account.MaxJobs) {
					return "AccountMaxJobs"
				}
			}
			if account.MaxSubmit > 0 {
				submitted := s.countSubmittedJobsForAccount(sub.ID, accountName)
				if submitted+1 > int64(account.MaxSubmit) {
					return "AccountMaxSubmit"
				}
			}
			if account.MaxBillingRunning > 0 {
				runningBilling := s.sumActiveBillingForAccount(sub.ID, accountName)
				if runningBilling+billingUnits > account.MaxBillingRunning {
					return "AccountBillingLimit"
				}
			}
			if account.MaxBillingSubmit > 0 {
				submitBilling := s.sumSubmittedBillingForAccount(sub.ID, accountName)
				if submitBilling+billingUnits > account.MaxBillingSubmit {
					return "AccountBillingSubmitLimit"
				}
			}
		}
	}

	if qosName == "" {
		return ""
	}
	qos, ok := s.qosByName(qosName)
	if !ok {
		return ""
	}
	if qos.MaxCPUPerJob > 0 && schedulingCPUForJob(job) > qos.MaxCPUPerJob {
		return "QOSMaxCPUPerJob"
	}
	if qos.MaxMemoryPerJob > 0 && EffectiveMemory(job.Problem, sub) > qos.MaxMemoryPerJob {
		return "QOSMaxMemoryPerJob"
	}
	if qos.MaxBillingPerJob > 0 && billingUnits > qos.MaxBillingPerJob {
		return "QOSMaxBillingPerJob"
	}
	if qos.MaxTime > 0 && timeLimit > qos.MaxTime {
		return "QOSMaxTime"
	}
	if qos.MaxJobsPerUser > 0 {
		running := s.countActiveJobsForUserQOS(sub.UserID, qosName)
		if running >= int64(qos.MaxJobsPerUser) {
			return "QOSMaxJobsPerUser"
		}
	}
	if qos.MaxBillingPerUserRunning > 0 {
		runningBilling := s.sumActiveBillingForUserQOS(sub.ID, sub.UserID, qosName)
		if runningBilling+billingUnits > qos.MaxBillingPerUserRunning {
			return "QOSBillingLimit"
		}
	}
	if qos.MaxSubmitJobsPerUser > 0 {
		submitted := s.countSubmittedJobsForUserQOS(sub.ID, sub.UserID, qosName)
		if submitted+1 > int64(qos.MaxSubmitJobsPerUser) {
			return "QOSMaxSubmitJobsPerUser"
		}
	}
	if qos.MaxBillingPerUserSubmit > 0 {
		submitBilling := s.sumSubmittedBillingForUserQOS(sub.ID, sub.UserID, qosName)
		if submitBilling+billingUnits > qos.MaxBillingPerUserSubmit {
			return "QOSBillingSubmitLimit"
		}
	}
	return ""
}

func (s *Scheduler) evaluateDependencies(sub *models.Submission) (jobDecision, string) {
	if sub == nil || strings.TrimSpace(sub.Dependencies) == "" {
		return jobDecisionRun, ""
	}
	if strings.Contains(sub.Dependencies, "?") {
		return s.evaluateDependencyAlternatives(sub)
	}
	return s.evaluateDependencyGroup(sub, splitDependencyClauses(sub.Dependencies))
}

func (s *Scheduler) evaluateDependencyAlternatives(sub *models.Submission) (jobDecision, string) {
	alternatives := strings.Split(sub.Dependencies, "?")
	anyWait := false
	failReason := "DependencyNeverSatisfied"
	for _, alternative := range alternatives {
		deps := splitDependencyClauses(alternative)
		if len(deps) == 0 {
			return jobDecisionFail, "InvalidDependency"
		}
		decision, reason := s.evaluateDependencyGroup(sub, deps)
		switch decision {
		case jobDecisionRun:
			return jobDecisionRun, ""
		case jobDecisionWait:
			anyWait = true
		case jobDecisionFail:
			if reason == "InvalidDependency" {
				return jobDecisionFail, reason
			}
			if reason != "" {
				failReason = reason
			}
		}
	}
	if anyWait {
		return jobDecisionWait, "Dependency"
	}
	return jobDecisionFail, failReason
}

func (s *Scheduler) evaluateDependencyGroup(sub *models.Submission, deps []string) (jobDecision, string) {
	for _, dep := range deps {
		if dep == "singleton" {
			count := s.countSingletonDependencies(sub)
			if count > 0 {
				return jobDecisionWait, "Dependency"
			}
			continue
		}

		depType, depIDs, ok := parseDependencySpec(dep)
		if !ok {
			return jobDecisionFail, "InvalidDependency"
		}
		for _, depID := range depIDs {
			if decision, reason := s.evaluateDependency(sub, depType, depID); decision != jobDecisionRun {
				return decision, reason
			}
		}
	}
	return jobDecisionRun, ""
}

func (s *Scheduler) countSingletonDependencies(sub *models.Submission) int64 {
	if sub == nil {
		return 0
	}
	name := dependencyJobName(sub)
	if name == "" {
		return 0
	}
	return s.countSubmissions(
		"id <> ? AND user_id = ? AND status IN ? AND (job_name = ? OR (job_name = '' AND problem_id = ?))",
		sub.ID,
		sub.UserID,
		submitStatuses(),
		name,
		name,
	)
}

func dependencyJobName(sub *models.Submission) string {
	if sub == nil {
		return ""
	}
	if name := strings.TrimSpace(sub.JobName); name != "" {
		return name
	}
	if name := strings.TrimSpace(sub.ProblemID); name != "" {
		return name
	}
	return strings.TrimSpace(sub.ID)
}

func parseDependencySpec(dep string) (string, []string, bool) {
	dep = strings.TrimSpace(dep)
	if dep == "" {
		return "", nil, false
	}
	depType, depIDsText, ok := strings.Cut(dep, ":")
	if !ok {
		return "afterok", []string{dep}, true
	}
	depType = strings.TrimSpace(depType)
	depIDs := make([]string, 0)
	for _, part := range splitDependencyIDs(depIDsText) {
		if id := strings.TrimSpace(part); id != "" {
			depIDs = append(depIDs, id)
		}
	}
	return depType, depIDs, depType != "" && len(depIDs) > 0
}

func splitDependencyClauses(value string) []string {
	return splitDependencyText(value, func(r rune) bool {
		return r == ',' || r == ';' || r == ' '
	})
}

func splitDependencyIDs(value string) []string {
	return splitDependencyText(value, func(r rune) bool {
		return r == ':'
	})
}

func splitDependencyText(value string, separator func(rune) bool) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := make([]string, 0)
	var current strings.Builder
	bracketDepth := 0
	for _, r := range value {
		switch r {
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		}
		if bracketDepth == 0 && separator(r) {
			if part := strings.TrimSpace(current.String()); part != "" {
				parts = append(parts, part)
			}
			current.Reset()
			continue
		}
		current.WriteRune(r)
	}
	if part := strings.TrimSpace(current.String()); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func (s *Scheduler) evaluateDependency(sub *models.Submission, depType, depID string) (jobDecision, string) {
	if depType == "aftercorr" {
		return s.evaluateAfterCorrDependency(sub, depID)
	}
	return s.evaluateSingleDependency(depType, depID)
}

func (s *Scheduler) evaluateAfterCorrDependency(sub *models.Submission, depArrayJobID string) (jobDecision, string) {
	if sub == nil || sub.ArrayJobID == "" {
		return jobDecisionFail, "InvalidDependency"
	}
	var depSub models.Submission
	err := s.db.Select("id", "status").
		Where("array_job_id = ? AND array_task_id = ?", depArrayJobID, sub.ArrayTaskID).
		First(&depSub).Error
	if err != nil {
		return jobDecisionWait, "Dependency"
	}
	return evaluateDependencyStatusAfterOK(depSub.Status)
}

func (s *Scheduler) evaluateSingleDependency(depType, depID string) (jobDecision, string) {
	targets, complete, invalid := s.dependencyTargets(depID)
	if invalid {
		return jobDecisionFail, "InvalidDependency"
	}
	if len(targets) == 0 {
		return jobDecisionWait, "Dependency"
	}
	anyWait := !complete
	for _, depSub := range targets {
		decision, reason := evaluateDependencyTargetStatus(depType, depSub.Status)
		switch decision {
		case jobDecisionFail:
			return decision, reason
		case jobDecisionWait:
			anyWait = true
		}
	}
	if anyWait {
		return jobDecisionWait, "Dependency"
	}
	return jobDecisionRun, ""
}

func evaluateDependencyTargetStatus(depType string, status models.Status) (jobDecision, string) {
	switch depType {
	case "after", "afterstart":
		if status == models.StatusQueued {
			return jobDecisionWait, "Dependency"
		}
	case "afterany":
		if !submissionFinished(status) {
			return jobDecisionWait, "Dependency"
		}
	case "afternotok":
		if status == models.StatusFailed {
			return jobDecisionRun, ""
		}
		if status == models.StatusSuccess {
			return jobDecisionFail, "DependencyNeverSatisfied"
		}
		return jobDecisionWait, "Dependency"
	case "afterok", "":
		return evaluateDependencyStatusAfterOK(status)
	default:
		return jobDecisionFail, "InvalidDependency"
	}
	return jobDecisionRun, ""
}

func (s *Scheduler) dependencyTargets(depID string) ([]models.Submission, bool, bool) {
	depID = strings.TrimSpace(depID)
	if depID == "" {
		return nil, false, true
	}

	if selector, ok, invalid := parseDependencyArrayTaskSelector(depID); ok || invalid {
		if invalid {
			return nil, false, true
		}
		var targets []models.Submission
		if err := s.db.Select("id", "status", "array_task_id").
			Where("array_job_id = ? AND array_task_id IN ?", selector.ArrayJobID, selector.TaskIDs).
			Order("array_task_id asc").
			Find(&targets).Error; err != nil {
			return nil, false, false
		}
		return targets, len(targets) == len(selector.TaskIDs), false
	}

	var arrayTargets []models.Submission
	if err := s.db.Select("id", "status", "array_task_id").
		Where("array_job_id = ?", depID).
		Order("array_task_id asc").
		Find(&arrayTargets).Error; err != nil {
		return nil, false, false
	}
	if len(arrayTargets) > 0 {
		return arrayTargets, true, false
	}

	var depSub models.Submission
	err := s.db.Select("id", "status").Where("id = ?", depID).First(&depSub).Error
	if err != nil {
		return nil, false, false
	}
	return []models.Submission{depSub}, true, false
}

type dependencyArrayTaskSelector struct {
	ArrayJobID string
	TaskIDs    []int
}

func parseDependencyArrayTaskSelector(value string) (dependencyArrayTaskSelector, bool, bool) {
	value = strings.TrimSpace(value)
	idx := strings.LastIndex(value, "_")
	if idx <= 0 || idx >= len(value)-1 {
		return dependencyArrayTaskSelector{}, false, false
	}

	taskSpec := strings.TrimSpace(value[idx+1:])
	if strings.HasPrefix(taskSpec, "[") && strings.HasSuffix(taskSpec, "]") {
		taskSpec = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(taskSpec, "["), "]"))
	}
	arrayJobID := strings.TrimSpace(value[:idx])
	if arrayJobID == "" || taskSpec == "" {
		return dependencyArrayTaskSelector{}, false, false
	}

	array, err := ParseJobArray(taskSpec)
	if err != nil {
		if looksLikeDependencyArrayTaskSpec(taskSpec) {
			return dependencyArrayTaskSelector{}, true, true
		}
		return dependencyArrayTaskSelector{}, false, false
	}
	return dependencyArrayTaskSelector{ArrayJobID: arrayJobID, TaskIDs: array.TaskIDs}, true, false
}

func looksLikeDependencyArrayTaskSpec(value string) bool {
	hasDigit := false
	for _, r := range value {
		switch {
		case r >= '0' && r <= '9':
			hasDigit = true
		case r == '-' || r == ':' || r == ',' || r == '%' || r == '[' || r == ']':
		default:
			return false
		}
	}
	return hasDigit
}

func evaluateDependencyStatusAfterOK(status models.Status) (jobDecision, string) {
	if status == models.StatusSuccess {
		return jobDecisionRun, ""
	}
	if status == models.StatusFailed {
		return jobDecisionFail, "DependencyNeverSatisfied"
	}
	return jobDecisionWait, "Dependency"
}

func (s *Scheduler) evaluateReservation(clusterName string, job QueuedSubmission, username, account string, now time.Time) (jobDecision, string) {
	reservationName := effectiveReservation(job)
	if reservationName == "" {
		return jobDecisionRun, ""
	}

	reservation, ok := s.reservationByName(reservationName)
	if !ok {
		return jobDecisionFail, "InvalidReservation"
	}
	if reservation.Cluster != "" && reservation.Cluster != clusterName {
		return jobDecisionFail, "InvalidReservation"
	}
	if !reservation.StartTime.IsZero() && now.Before(reservation.StartTime) {
		return jobDecisionWait, "ReservationTime"
	}
	if !reservation.EndTime.IsZero() && now.After(reservation.EndTime) {
		return jobDecisionFail, "ReservationExpired"
	}
	if len(reservation.Users) > 0 && !stringAllowed(username, reservation.Users) && !stringAllowed(job.Submission.UserID, reservation.Users) {
		return jobDecisionFail, "ReservationUserLimit"
	}
	if account != "" && len(reservation.Accounts) > 0 && !stringAllowed(account, reservation.Accounts) {
		return jobDecisionFail, "ReservationAccountLimit"
	}
	return jobDecisionRun, ""
}

func (s *Scheduler) findAvailableNodeForJob(clusterName string, job QueuedSubmission, now time.Time) (*NodeState, []int) {
	allocations := s.findAvailableNodeAllocationsForJob(clusterName, job, now)
	if len(allocations) == 0 {
		return nil, nil
	}
	return allocations[0].Node, allocations[0].Cores
}

func (s *Scheduler) findAvailableNodeAllocationsForJob(clusterName string, job QueuedSubmission, now time.Time) []nodeAllocation {
	requiredCPU := schedulingCPUForJob(job)
	requiredMemoryPerNode := EffectiveMemory(job.Problem, job.Submission)
	nodeCount := effectiveNodeCount(job)
	reservationName := effectiveReservation(job)
	requiredReservationMemory := requiredMemoryPerNode * int64(nodeCount)
	if reservationName == "" {
		return s.allocateNodes(clusterName, nodeCount, requiredCPU, requiredMemoryPerNode, job.Submission.ID, job.Submission.Exclusive, func(node *NodeState) bool {
			return s.nodeMatchesJob(clusterName, node, job, now)
		})
	}

	reservation, ok := s.reservationByName(reservationName)
	if !ok {
		return nil
	}

	s.reservationMu.Lock()
	defer s.reservationMu.Unlock()
	if !s.reservationCapacityAvailableLocked(clusterName, reservation, requiredCPU, requiredReservationMemory) {
		return nil
	}

	allocations := s.allocateNodes(clusterName, nodeCount, requiredCPU, requiredMemoryPerNode, job.Submission.ID, job.Submission.Exclusive, func(node *NodeState) bool {
		return s.nodeMatchesJob(clusterName, node, job, now)
	})
	if len(allocations) == 0 {
		return nil
	}
	if job.Submission.ID != "" {
		s.reservationUsed[job.Submission.ID] = reservationAllocation{
			Cluster:     clusterName,
			Reservation: reservation.Name,
			CPU:         requiredCPU,
			Memory:      requiredReservationMemory,
		}
	}
	return allocations
}

func effectiveNodeCount(job QueuedSubmission) int {
	if job.Submission != nil && job.Submission.Nodes > 0 {
		return job.Submission.Nodes
	}
	return 1
}

func schedulingCPUForJob(job QueuedSubmission) int {
	return schedulingCPUForSubmission(job.Problem, job.Submission)
}

func schedulingCPUForSubmission(problem *Problem, sub *models.Submission) int {
	cpu := EffectiveCPU(problem, sub)
	nodes := 1
	if sub != nil && sub.Nodes > 0 {
		nodes = sub.Nodes
	}
	if nodes > 1 && cpu < nodes {
		return nodes
	}
	return cpu
}

func (s *Scheduler) allocateNodes(clusterName string, nodeCount int, totalCPU int, memoryPerNode int64, owner string, exclusive bool, filter nodeFilter) []nodeAllocation {
	if nodeCount <= 1 {
		node, cores := s.allocateNode(clusterName, totalCPU, memoryPerNode, owner, exclusive, filter)
		if node == nil {
			return nil
		}
		return []nodeAllocation{{Node: node, Cores: cores, CPU: totalCPU, Memory: memoryPerNode}}
	}
	if totalCPU < nodeCount {
		totalCPU = nodeCount
	}
	cpuShares := distributeAcrossNodes(totalCPU, nodeCount)
	allocations := make([]nodeAllocation, 0, nodeCount)
	selected := make(map[string]bool, nodeCount)
	for i := 0; i < nodeCount; i++ {
		share := cpuShares[i]
		node, cores := s.allocateNode(clusterName, share, memoryPerNode, owner, exclusive, func(node *NodeState) bool {
			if selected[node.Name] {
				return false
			}
			if filter != nil && !filter(node) {
				return false
			}
			return true
		})
		if node == nil {
			s.releaseNodeAllocations(clusterName, allocations, owner, false)
			return nil
		}
		selected[node.Name] = true
		allocations = append(allocations, nodeAllocation{Node: node, Cores: cores, CPU: share, Memory: memoryPerNode})
	}
	return allocations
}

func distributeAcrossNodes(total, nodes int) []int {
	if nodes <= 0 {
		return nil
	}
	if total < nodes {
		total = nodes
	}
	base := total / nodes
	remainder := total % nodes
	out := make([]int, nodes)
	for i := range out {
		out[i] = base
		if i < remainder {
			out[i]++
		}
	}
	return out
}

func (s *Scheduler) allocateNode(clusterName string, requiredCPU int, requiredMemory int64, owner string, exclusive bool, filter nodeFilter) (*NodeState, []int) {
	cluster, ok := s.clusters[clusterName]
	if !ok {
		return nil, nil
	}

	cluster.Lock()
	defer cluster.Unlock()

	nodes := make([]*NodeState, 0, len(cluster.Nodes))
	for _, node := range cluster.Nodes {
		nodes = append(nodes, node)
	}
	sort.SliceStable(nodes, func(i, j int) bool {
		if nodes[i].Weight == nodes[j].Weight {
			return nodes[i].Name < nodes[j].Name
		}
		return nodes[i].Weight < nodes[j].Weight
	})

	for _, node := range nodes {
		node.Lock()
		if !nodeStateSchedulable(node) || (filter != nil && !filter(node)) {
			node.Unlock()
			continue
		}
		if node.ExclusiveOwner != "" {
			node.Unlock()
			continue
		}
		if exclusive && nodeHasAllocations(node) {
			node.Unlock()
			continue
		}
		if requiredCPU > len(node.UsedCores) {
			node.Unlock()
			continue
		}
		if node.Memory-node.UsedMemory < requiredMemory {
			node.Unlock()
			continue
		}

		startCore := -1
		if requiredCPU > 0 {
			for i := 0; i <= len(node.UsedCores)-requiredCPU; i++ {
				if coresFree(node.UsedCores, i, requiredCPU) {
					startCore = i
					break
				}
			}
		} else {
			startCore = -2
		}

		if startCore == -1 {
			node.Unlock()
			continue
		}

		allocatedCores := make([]int, 0, requiredCPU)
		if exclusive {
			allocatedCores = make([]int, 0, len(node.UsedCores))
			for coreID := range node.UsedCores {
				node.UsedCores[coreID] = true
				if coreID < len(node.UsedCoreOwners) {
					node.UsedCoreOwners[coreID] = owner
				}
				allocatedCores = append(allocatedCores, coreID)
			}
			node.ExclusiveOwner = owner
		} else if startCore != -2 {
			for i := 0; i < requiredCPU; i++ {
				coreID := startCore + i
				node.UsedCores[coreID] = true
				if coreID < len(node.UsedCoreOwners) {
					node.UsedCoreOwners[coreID] = owner
				}
				allocatedCores = append(allocatedCores, coreID)
			}
		}
		node.UsedMemory += requiredMemory
		if owner != "" {
			if node.MemoryAllocations == nil {
				node.MemoryAllocations = make(map[string]int64)
			}
			node.MemoryAllocations[owner] += requiredMemory
		}
		node.Unlock()
		return node, allocatedCores
	}
	return nil, nil
}

func (s *Scheduler) nodeMatchesJob(clusterName string, node *NodeState, job QueuedSubmission, now time.Time) bool {
	if !nodeMatchesRequestedLists(node.Name, effectiveNodeList(job), effectiveExcludeNodes(job)) {
		return false
	}
	if !nodeHasFeatures(node.Features, effectiveConstraint(job)) {
		return false
	}
	if !nodeHasGRES(node.GRES, effectiveGRES(job)) {
		return false
	}

	reservationName := effectiveReservation(job)
	if reservationName != "" {
		reservation, ok := s.reservationByName(reservationName)
		if !ok {
			return false
		}
		return reservationMatchesNode(reservation, clusterName, node.Name)
	}

	for _, reservation := range s.activeReservations(clusterName, now) {
		if reservation.AllowOverlap {
			continue
		}
		if reservationMatchesNode(reservation, clusterName, node.Name) {
			return false
		}
	}
	return true
}

func clusterStateSchedulable(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "", "up", "idle", "mixed", "alloc":
		return true
	case "down", "drain", "drained", "inactive":
		return false
	default:
		return false
	}
}

func nodeStateSchedulable(node *NodeState) bool {
	if node == nil || node.IsPaused {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(node.State)) {
	case "", "up", "idle", "mixed", "alloc":
		return true
	case "down", "drain", "drained", "fail", "maintenance", "reserved":
		return false
	default:
		return false
	}
}

func nodeHasAllocations(node *NodeState) bool {
	if node == nil {
		return false
	}
	if node.UsedMemory > 0 || len(node.MemoryAllocations) > 0 {
		return true
	}
	for _, used := range node.UsedCores {
		if used {
			return true
		}
	}
	return false
}

func coresFree(used []bool, start, count int) bool {
	for i := 0; i < count; i++ {
		if used[start+i] {
			return false
		}
	}
	return true
}

func (s *Scheduler) qosByName(name string) (config.QOS, bool) {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	for _, qos := range s.cfg.Scheduler.QOS {
		if qos.Name == name {
			return cloneQOS(qos), true
		}
	}
	return config.QOS{}, false
}

func (s *Scheduler) hasQOSConfigured() bool {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return len(s.cfg.Scheduler.QOS) > 0
}

func (s *Scheduler) accountByName(name string) (config.Account, bool) {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	for _, account := range s.cfg.Scheduler.Accounts {
		if account.Name == name {
			return cloneAccount(account), true
		}
	}
	return config.Account{}, false
}

func (s *Scheduler) hasAccountsConfigured() bool {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return len(s.cfg.Scheduler.Accounts) > 0
}

func (s *Scheduler) reservationByName(name string) (config.Reservation, bool) {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	for _, reservation := range s.cfg.Scheduler.Reservations {
		if reservation.Name == name {
			return cloneReservation(reservation), true
		}
	}
	return config.Reservation{}, false
}

func (s *Scheduler) activeReservations(clusterName string, now time.Time) []config.Reservation {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	reservations := make([]config.Reservation, 0)
	for _, reservation := range s.cfg.Scheduler.Reservations {
		if reservation.Cluster != "" && reservation.Cluster != clusterName {
			continue
		}
		if !reservation.StartTime.IsZero() && now.Before(reservation.StartTime) {
			continue
		}
		if !reservation.EndTime.IsZero() && now.After(reservation.EndTime) {
			continue
		}
		reservations = append(reservations, cloneReservation(reservation))
	}
	return reservations
}

func reservationMatchesNode(reservation config.Reservation, clusterName, nodeName string) bool {
	if reservation.Cluster != "" && reservation.Cluster != clusterName {
		return false
	}
	if len(reservation.Nodes) == 0 {
		return true
	}
	return stringAllowed(nodeName, reservation.Nodes)
}

func (s *Scheduler) reservationCapacityAvailableLocked(clusterName string, reservation config.Reservation, requiredCPU int, requiredMemory int64) bool {
	usedCPU, usedMemory := s.reservationUsageLocked(clusterName, reservation.Name)
	if reservation.CPU > 0 && usedCPU+requiredCPU > reservation.CPU {
		return false
	}
	if reservation.Memory > 0 && usedMemory+requiredMemory > reservation.Memory {
		return false
	}
	return true
}

func (s *Scheduler) reservationUsageLocked(clusterName, reservationName string) (int, int64) {
	usedCPU := 0
	var usedMemory int64
	for _, allocation := range s.reservationUsed {
		if allocation.Cluster != clusterName || allocation.Reservation != reservationName {
			continue
		}
		usedCPU += allocation.CPU
		usedMemory += allocation.Memory
	}
	return usedCPU, usedMemory
}

func (s *Scheduler) lookupUser(userID string) (*models.User, string) {
	user, err := database.GetUserByID(s.db, userID)
	if err != nil {
		return &models.User{ID: userID, Username: userID}, userID
	}
	return user, user.Username
}

func (s *Scheduler) countSubmissions(query string, args ...interface{}) int64 {
	var count int64
	if err := s.db.Model(&models.Submission{}).Where(query, args...).Count(&count).Error; err != nil {
		return 0
	}
	return count
}

func (s *Scheduler) countAllocations(query string, args ...interface{}) int64 {
	var count int64
	if err := s.db.Model(&models.Allocation{}).Where(query, args...).Count(&count).Error; err != nil {
		return 0
	}
	return count
}

func (s *Scheduler) countActiveJobsForCluster(clusterName string) int64 {
	return s.countSubmissions("cluster = ? AND status IN ?", clusterName, activeStatuses()) +
		s.countAllocations("cluster = ? AND status = ?", clusterName, models.AllocationActive)
}

func (s *Scheduler) countActiveJobsForAccount(accountName string) int64 {
	return s.countSubmissions("account = ? AND status IN ?", accountName, activeStatuses()) +
		s.countAllocations("account = ? AND status = ?", accountName, models.AllocationActive)
}

func (s *Scheduler) countSubmittedJobsForAccount(ownerID, accountName string) int64 {
	return s.countSubmissions("id <> ? AND account = ? AND status IN ?", ownerID, accountName, submitStatuses()) +
		s.countAllocations("id <> ? AND account = ? AND status = ?", ownerID, accountName, models.AllocationActive)
}

func (s *Scheduler) countActiveJobsForUserQOS(userID, qosName string) int64 {
	return s.countSubmissions("user_id = ? AND qos = ? AND status IN ?", userID, qosName, activeStatuses()) +
		s.countAllocations("user_id = ? AND qos = ? AND status = ?", userID, qosName, models.AllocationActive)
}

func (s *Scheduler) countSubmittedJobsForUserQOS(ownerID, userID, qosName string) int64 {
	return s.countSubmissions("id <> ? AND user_id = ? AND qos = ? AND status IN ?", ownerID, userID, qosName, submitStatuses()) +
		s.countAllocations("id <> ? AND user_id = ? AND qos = ? AND status = ?", ownerID, userID, qosName, models.AllocationActive)
}

func (s *Scheduler) sumSubmissionBilling(query string, args ...interface{}) float64 {
	var total float64
	if err := s.db.Model(&models.Submission{}).
		Select("COALESCE(SUM(billing_units), 0)").
		Where(query, args...).
		Scan(&total).Error; err != nil {
		return 0
	}
	return total
}

func (s *Scheduler) sumAllocationBilling(query string, args ...interface{}) float64 {
	var total float64
	if err := s.db.Model(&models.Allocation{}).
		Select("COALESCE(SUM(billing_units), 0)").
		Where(query, args...).
		Scan(&total).Error; err != nil {
		return 0
	}
	return total
}

func (s *Scheduler) sumActiveBillingForAccount(ownerID, accountName string) float64 {
	return s.sumSubmissionBilling("id <> ? AND account = ? AND status IN ?", ownerID, accountName, activeStatuses()) +
		s.sumAllocationBilling("id <> ? AND account = ? AND status = ?", ownerID, accountName, models.AllocationActive)
}

func (s *Scheduler) sumSubmittedBillingForAccount(ownerID, accountName string) float64 {
	return s.sumSubmissionBilling("id <> ? AND account = ? AND status IN ?", ownerID, accountName, submitStatuses()) +
		s.sumAllocationBilling("id <> ? AND account = ? AND status = ?", ownerID, accountName, models.AllocationActive)
}

func (s *Scheduler) sumActiveBillingForUserQOS(ownerID, userID, qosName string) float64 {
	return s.sumSubmissionBilling("id <> ? AND user_id = ? AND qos = ? AND status IN ?", ownerID, userID, qosName, activeStatuses()) +
		s.sumAllocationBilling("id <> ? AND user_id = ? AND qos = ? AND status = ?", ownerID, userID, qosName, models.AllocationActive)
}

func (s *Scheduler) sumSubmittedBillingForUserQOS(ownerID, userID, qosName string) float64 {
	return s.sumSubmissionBilling("id <> ? AND user_id = ? AND qos = ? AND status IN ?", ownerID, userID, qosName, submitStatuses()) +
		s.sumAllocationBilling("id <> ? AND user_id = ? AND qos = ? AND status = ?", ownerID, userID, qosName, models.AllocationActive)
}

func effectiveAccount(job QueuedSubmission) string {
	if job.Submission.Account != "" {
		return job.Submission.Account
	}
	return job.Problem.Scheduling.Account
}

func effectiveQOS(job QueuedSubmission) string {
	if job.Submission.QOS != "" {
		return job.Submission.QOS
	}
	return job.Problem.Scheduling.QOS
}

func effectiveNice(job QueuedSubmission) int {
	if job.Submission.Nice != 0 {
		return job.Submission.Nice
	}
	return job.Problem.Scheduling.Nice
}

func effectiveTimeLimit(job QueuedSubmission) int {
	if job.Submission.TimeLimit > 0 {
		return job.Submission.TimeLimit
	}
	if job.Problem.Scheduling.TimeLimit > 0 {
		return job.Problem.Scheduling.TimeLimit
	}
	total := 0
	for _, step := range job.Problem.Workflow {
		total += step.Timeout
	}
	return total
}

func effectiveReservation(job QueuedSubmission) string {
	if job.Submission.Reservation != "" {
		return job.Submission.Reservation
	}
	return job.Problem.Scheduling.Reservation
}

func effectiveNodeList(job QueuedSubmission) string {
	if job.Submission.NodeList != "" {
		return job.Submission.NodeList
	}
	return job.Problem.Scheduling.NodeList
}

func effectiveExcludeNodes(job QueuedSubmission) string {
	if job.Submission.ExcludeNodes != "" {
		return job.Submission.ExcludeNodes
	}
	return job.Problem.Scheduling.ExcludeNodes
}

func effectiveConstraint(job QueuedSubmission) string {
	if job.Submission.Constraint != "" {
		return job.Submission.Constraint
	}
	return job.Problem.Scheduling.Constraint
}

func effectiveGRES(job QueuedSubmission) string {
	if job.Submission.GRES != "" {
		return job.Submission.GRES
	}
	return job.Problem.Scheduling.GRES
}

func submissionFinished(status models.Status) bool {
	return status == models.StatusSuccess || status == models.StatusFailed
}

func submissionActive(status models.Status) bool {
	return status == models.StatusRunning || status == models.StatusSuspended
}

func activeStatuses() []models.Status {
	return []models.Status{models.StatusRunning, models.StatusSuspended}
}

func submitStatuses() []models.Status {
	return []models.Status{models.StatusQueued, models.StatusRunning, models.StatusSuspended}
}

func splitList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';' || r == ' '
	})
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		if trimmed := strings.TrimSpace(field); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func nodeMatchesRequestedLists(nodeName, requested, excluded string) bool {
	requestedNodes := expandRequestedNodeList(requested)
	if len(requestedNodes) > 0 && !nodeNameInList(nodeName, requestedNodes) {
		return false
	}
	return !nodeNameInList(nodeName, expandRequestedNodeList(excluded))
}

func nodeNameInList(nodeName string, nodes []string) bool {
	for _, candidate := range nodes {
		if candidate == nodeName {
			return true
		}
	}
	return false
}

func expandRequestedNodeList(value string) []string {
	parts := splitRequestedNodeList(value)
	if len(parts) == 0 {
		return nil
	}
	out := make([]string, 0, len(parts))
	seen := make(map[string]bool, len(parts))
	for _, part := range parts {
		for _, nodeName := range expandRequestedNodePattern(part) {
			if nodeName == "" || seen[nodeName] {
				continue
			}
			seen[nodeName] = true
			out = append(out, nodeName)
		}
	}
	return out
}

func splitRequestedNodeList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	var parts []string
	var current strings.Builder
	depth := 0
	flush := func() {
		if part := strings.TrimSpace(current.String()); part != "" {
			parts = append(parts, part)
		}
		current.Reset()
	}
	for _, r := range value {
		switch r {
		case '[':
			depth++
			current.WriteRune(r)
		case ']':
			if depth > 0 {
				depth--
			}
			current.WriteRune(r)
		case ',', ';', ' ', '\t', '\n', '\r':
			if depth == 0 {
				flush()
				continue
			}
			current.WriteRune(r)
		default:
			current.WriteRune(r)
		}
	}
	flush()
	return parts
}

func expandRequestedNodePattern(pattern string) []string {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return nil
	}
	open := strings.Index(pattern, "[")
	if open == -1 {
		return []string{pattern}
	}
	close := strings.Index(pattern[open:], "]")
	if close == -1 {
		return []string{pattern}
	}
	close += open
	prefix := pattern[:open]
	body := pattern[open+1 : close]
	suffix := pattern[close+1:]
	values, ok := expandRequestedNodeRangeBody(body)
	if !ok || len(values) == 0 {
		return []string{pattern}
	}
	suffixes := expandRequestedNodePattern(suffix)
	if len(suffixes) == 0 {
		suffixes = []string{""}
	}
	out := make([]string, 0, len(values)*len(suffixes))
	for _, value := range values {
		for _, expandedSuffix := range suffixes {
			out = append(out, prefix+value+expandedSuffix)
		}
	}
	return out
}

func expandRequestedNodeRangeBody(body string) ([]string, bool) {
	parts := splitRequestedNodeList(body)
	if len(parts) == 0 {
		return nil, false
	}
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		values, ok := expandRequestedNodeRangePart(part)
		if !ok {
			return nil, false
		}
		out = append(out, values...)
	}
	return out, true
}

func expandRequestedNodeRangePart(part string) ([]string, bool) {
	part = strings.TrimSpace(part)
	if part == "" {
		return nil, false
	}
	rangePart, stepPart, _ := strings.Cut(part, ":")
	step := 1
	if stepPart != "" {
		parsedStep, err := strconv.Atoi(stepPart)
		if err != nil || parsedStep <= 0 {
			return nil, false
		}
		step = parsedStep
	}
	startRaw, endRaw, hasRange := strings.Cut(rangePart, "-")
	if !hasRange {
		return []string{rangePart}, true
	}
	start, err := strconv.Atoi(startRaw)
	if err != nil {
		return nil, false
	}
	end, err := strconv.Atoi(endRaw)
	if err != nil {
		return nil, false
	}
	width := len(startRaw)
	if len(endRaw) > width {
		width = len(endRaw)
	}
	out := make([]string, 0)
	if start <= end {
		for i := start; i <= end; i += step {
			out = append(out, fmt.Sprintf("%0*d", width, i))
		}
		return out, true
	}
	for i := start; i >= end; i -= step {
		out = append(out, fmt.Sprintf("%0*d", width, i))
	}
	return out, true
}

func stringAllowed(value string, allowed []string) bool {
	for _, item := range allowed {
		if item == "*" || item == value {
			return true
		}
	}
	return false
}

func nodeHasFeatures(features []string, constraint string) bool {
	required := splitConstraint(constraint)
	if len(required) == 0 {
		return true
	}
	featureSet := make(map[string]struct{}, len(features))
	for _, feature := range features {
		featureSet[strings.TrimSpace(feature)] = struct{}{}
	}
	for _, feature := range required {
		if _, ok := featureSet[feature]; !ok {
			return false
		}
	}
	return true
}

func splitConstraint(constraint string) []string {
	if strings.TrimSpace(constraint) == "" {
		return nil
	}
	fields := strings.FieldsFunc(constraint, func(r rune) bool {
		return r == '&' || r == ',' || r == ' '
	})
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			out = append(out, field)
		}
	}
	return out
}

func nodeHasGRES(nodeGRES []string, requested string) bool {
	requests := splitList(requested)
	if len(requests) == 0 {
		return true
	}
	available := parseGRESList(nodeGRES)
	for _, request := range requests {
		name, count, err := parseGRES(request)
		if err != nil {
			return false
		}
		if available[name] < count {
			return false
		}
	}
	return true
}

func parseGRESList(values []string) map[string]int {
	out := make(map[string]int, len(values))
	for _, value := range values {
		name, count, err := parseGRES(value)
		if err != nil {
			continue
		}
		out[name] += count
	}
	return out
}

func parseGRES(value string) (string, int, error) {
	parts := strings.Split(strings.TrimSpace(value), ":")
	if len(parts) == 0 || parts[0] == "" {
		return "", 0, fmt.Errorf("invalid gres %q", value)
	}
	name := parts[0]
	count := 1
	if len(parts) > 1 {
		parsed, err := strconv.Atoi(parts[len(parts)-1])
		if err == nil {
			count = parsed
		}
	}
	if count <= 0 {
		return "", 0, fmt.Errorf("invalid gres count %q", value)
	}
	return name, count, nil
}
