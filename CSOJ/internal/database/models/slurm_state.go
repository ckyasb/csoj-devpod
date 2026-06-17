package models

import "strings"

const (
	SlurmStatePending   = "PENDING"
	SlurmStateRunning   = "RUNNING"
	SlurmStateSuspended = "SUSPENDED"
	SlurmStateCompleted = "COMPLETED"
	SlurmStateFailed    = "FAILED"
	SlurmStateCancelled = "CANCELLED"
	SlurmStateDeadline  = "DEADLINE"
	SlurmStateNodeFail  = "NODE_FAIL"
	SlurmStateOOM       = "OUT_OF_MEMORY"
	SlurmStatePreempted = "PREEMPTED"
	SlurmStateTimeout   = "TIMEOUT"
	SlurmStateUnknown   = "UNKNOWN"
)

func DeriveSlurmJobState(status Status, hold bool, reason string) (string, string) {
	reason = strings.TrimSpace(reason)
	switch status {
	case StatusQueued:
		if hold && reason == "" {
			reason = "JobHeld"
		}
		if reason == "" {
			reason = "Priority"
		}
		return SlurmStatePending, reason
	case StatusRunning:
		return SlurmStateRunning, slurmReasonOrNone(reason)
	case StatusSuspended:
		return SlurmStateSuspended, slurmReasonOrNone(reason)
	case StatusSuccess:
		return SlurmStateCompleted, "None"
	case StatusFailed:
		return failedSlurmState(reason)
	default:
		return SlurmStateUnknown, slurmReasonOrNone(reason)
	}
}

func (s *Submission) PopulateSlurmState() {
	if s == nil {
		return
	}
	s.SlurmState, s.SlurmReason = DeriveSlurmJobState(s.Status, s.Hold, s.Reason)
}

func PopulateSlurmStateForSubmissions(submissions []Submission) {
	for i := range submissions {
		submissions[i].PopulateSlurmState()
	}
}

func failedSlurmState(reason string) (string, string) {
	switch reason {
	case "Preempted":
		return SlurmStatePreempted, "Preempted"
	case "Interrupted", "Cancelled", "Canceled":
		return SlurmStateCancelled, "Cancelled"
	case "Deadline":
		return SlurmStateDeadline, "Deadline"
	case "TimeLimit", "Timeout":
		return SlurmStateTimeout, "TimeLimit"
	case "NodeFail", "NodeDown":
		return SlurmStateNodeFail, reason
	case "OutOfMemory", "OOM":
		return SlurmStateOOM, "OutOfMemory"
	}
	if reason == "" {
		reason = "NonZeroExitCode"
	}
	return SlurmStateFailed, reason
}

func slurmReasonOrNone(reason string) string {
	if reason == "" {
		return "None"
	}
	return reason
}
