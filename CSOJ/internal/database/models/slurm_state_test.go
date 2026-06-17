package models

import "testing"

func TestDeriveSlurmJobState(t *testing.T) {
	tests := []struct {
		name       string
		status     Status
		hold       bool
		reason     string
		wantState  string
		wantReason string
	}{
		{
			name:       "held queued job",
			status:     StatusQueued,
			hold:       true,
			wantState:  SlurmStatePending,
			wantReason: "JobHeld",
		},
		{
			name:       "running job",
			status:     StatusRunning,
			wantState:  SlurmStateRunning,
			wantReason: "None",
		},
		{
			name:       "suspended job",
			status:     StatusSuspended,
			reason:     "Suspended",
			wantState:  SlurmStateSuspended,
			wantReason: "Suspended",
		},
		{
			name:       "successful job",
			status:     StatusSuccess,
			wantState:  SlurmStateCompleted,
			wantReason: "None",
		},
		{
			name:       "preempted job",
			status:     StatusFailed,
			reason:     "Preempted",
			wantState:  SlurmStatePreempted,
			wantReason: "Preempted",
		},
		{
			name:       "interrupted job",
			status:     StatusFailed,
			reason:     "Interrupted",
			wantState:  SlurmStateCancelled,
			wantReason: "Cancelled",
		},
		{
			name:       "failed job without reason",
			status:     StatusFailed,
			wantState:  SlurmStateFailed,
			wantReason: "NonZeroExitCode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, reason := DeriveSlurmJobState(tt.status, tt.hold, tt.reason)
			if state != tt.wantState || reason != tt.wantReason {
				t.Fatalf("state/reason = %s/%s, want %s/%s", state, reason, tt.wantState, tt.wantReason)
			}
		})
	}
}
