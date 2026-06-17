package judger

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"github.com/ZJUSCT/CSOJ/internal/pubsub"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func (s *Scheduler) tryPreemptForJob(clusterName string, job QueuedSubmission, now time.Time) (*NodeState, []int, bool) {
	allocations, preempted := s.tryPreemptForJobAllocations(clusterName, job, now)
	if len(allocations) == 0 {
		return nil, nil, preempted
	}
	return allocations[0].Node, allocations[0].Cores, preempted
}

func (s *Scheduler) tryPreemptForJobAllocations(clusterName string, job QueuedSubmission, now time.Time) ([]nodeAllocation, bool) {
	qosName := effectiveQOS(job)
	qos, ok := s.qosByName(qosName)
	if !ok || len(qos.Preempt) == 0 {
		return nil, false
	}

	var candidates []models.Submission
	if err := s.db.Preload("Containers").
		Where("cluster = ? AND status IN ? AND qos IN ?", clusterName, activeStatuses(), qos.Preempt).
		Find(&candidates).Error; err != nil {
		zap.S().Warnf("failed to query preemption candidates for submission %s: %v", job.Submission.ID, err)
		return nil, false
	}
	if len(candidates) == 0 {
		return nil, false
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return s.runningSubmissionPriority(clusterName, &candidates[i], now) < s.runningSubmissionPriority(clusterName, &candidates[j], now)
	})

	preemptedAny := false
	for i := range candidates {
		candidate := candidates[i]
		if candidate.ID == job.Submission.ID {
			continue
		}
		reason := fmt.Sprintf("Preempted by submission %s", job.Submission.ID)
		if err := s.preemptRunningSubmission(&candidate, reason); err != nil {
			zap.S().Warnf("failed to preempt submission %s for %s: %v", candidate.ID, job.Submission.ID, err)
			continue
		}
		preemptedAny = true
		allocations := s.findAvailableNodeAllocationsForJob(clusterName, job, now)
		if len(allocations) > 0 {
			return allocations, true
		}
	}
	return nil, preemptedAny
}

func (s *Scheduler) runningSubmissionPriority(clusterName string, sub *models.Submission, now time.Time) int {
	if sub == nil {
		return 0
	}
	s.appState.RLock()
	problem := s.appState.Problems[sub.ProblemID]
	s.appState.RUnlock()
	if problem == nil {
		problem = &Problem{CPU: 0}
	}
	return s.jobPriority(clusterName, QueuedSubmission{Submission: sub, Problem: problem}, now)
}

func (s *Scheduler) preemptRunningSubmission(sub *models.Submission, reason string) error {
	if sub == nil {
		return fmt.Errorf("submission is nil")
	}
	if !submissionActive(sub.Status) {
		return nil
	}

	if err := s.cleanupRunningContainers(sub); err != nil {
		return err
	}

	err := s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.Submission{}).Where("id = ?", sub.ID).Updates(map[string]interface{}{
			"status": models.StatusFailed,
			"reason": "Preempted",
			"info":   models.JSONMap{"error": reason},
		}).Error; err != nil {
			return err
		}
		return tx.Model(&models.Container{}).
			Where("submission_id = ? AND status IN ?", sub.ID, activeStatuses()).
			Updates(map[string]interface{}{
				"status":    models.StatusFailed,
				"exit_code": -1,
			}).Error
	})
	if err != nil {
		return err
	}

	coresToRelease := parseAllocatedCores(sub.AllocatedCores)
	s.ReleaseResources(sub.Cluster, sub.Node, coresToRelease, s.submissionMemory(sub), sub.ID)

	sub.Status = models.StatusFailed
	sub.Reason = "Preempted"
	sub.Info = models.JSONMap{"error": reason}
	record := database.AccountingFromSubmission(sub, database.AccountEventPreempted)
	record.Message = reason
	if err := database.RecordAccounting(s.db, record); err != nil {
		zap.S().Warnf("failed to record preemption accounting for submission %s: %v", sub.ID, err)
	}

	msg := pubsub.FormatMessage("error", reason)
	pubsub.GetBroker().Publish(sub.ID, msg)
	pubsub.GetBroker().CloseTopic(sub.ID)
	if sub.Requeue {
		if err := s.requeuePreemptedSubmission(sub); err != nil {
			zap.S().Warnf("failed to requeue preempted submission %s: %v", sub.ID, err)
		}
	} else {
		s.notifySubmissionMail(sub, SlurmMailFail, reason)
	}
	return nil
}

func (s *Scheduler) requeuePreemptedSubmission(sub *models.Submission) error {
	if sub == nil {
		return fmt.Errorf("submission is nil")
	}

	s.appState.RLock()
	problem := s.appState.Problems[sub.ProblemID]
	s.appState.RUnlock()
	if problem == nil {
		return fmt.Errorf("problem %q not found", sub.ProblemID)
	}

	err := s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("submission_id = ?", sub.ID).Delete(&models.Container{}).Error; err != nil {
			return err
		}
		return tx.Model(&models.Submission{}).Where("id = ?", sub.ID).Updates(map[string]interface{}{
			"status":               models.StatusQueued,
			"current_step":         0,
			"node":                 "",
			"allocated_cores":      "",
			"allocated_node_cores": "",
			"score":                0,
			"performance":          0,
			"info":                 models.JSONMap{},
			"reason":               "",
		}).Error
	})
	if err != nil {
		return err
	}

	sub.Status = models.StatusQueued
	sub.CurrentStep = 0
	sub.Node = ""
	sub.AllocatedCores = ""
	sub.AllocatedNodeCores = ""
	sub.Score = 0
	sub.Performance = 0
	sub.Info = models.JSONMap{}
	sub.Reason = ""
	if err := database.RecordAccounting(s.db, database.AccountingFromSubmission(sub, database.AccountEventRequeued)); err != nil {
		zap.S().Warnf("failed to record accounting requeue event for preempted submission %s: %v", sub.ID, err)
	}
	s.notifySubmissionMail(sub, SlurmMailRequeue, "Preempted and requeued")
	s.Submit(sub, problem)
	return nil
}

func (s *Scheduler) cleanupRunningContainers(sub *models.Submission) error {
	containers := make([]models.Container, 0, len(sub.Containers))
	for _, container := range sub.Containers {
		if container.DockerID != "" {
			containers = append(containers, container)
		}
	}
	if len(containers) == 0 {
		return nil
	}
	return CleanupRuntimeContainers(s.cfg, sub.Cluster, sub.Node, containers)
}

func (s *Scheduler) submissionMemory(sub *models.Submission) int64 {
	if sub == nil {
		return 0
	}
	s.appState.RLock()
	defer s.appState.RUnlock()
	if problem, ok := s.appState.Problems[sub.ProblemID]; ok {
		return EffectiveMemory(problem, sub)
	}
	if sub.Memory > 0 {
		return sub.Memory
	}
	return 0
}

func parseAllocatedCores(value string) []int {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	cores := make([]int, 0, len(parts))
	for _, part := range parts {
		coreID, err := strconv.Atoi(strings.TrimSpace(part))
		if err == nil {
			cores = append(cores, coreID)
		}
	}
	return cores
}
