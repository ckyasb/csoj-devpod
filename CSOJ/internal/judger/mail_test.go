package judger

import (
	"strings"
	"sync"
	"testing"

	"github.com/ZJUSCT/CSOJ/internal/database/models"
)

type fakeMailSender struct {
	mu       sync.Mutex
	messages []SlurmMailMessage
}

func (f *fakeMailSender) Send(message SlurmMailMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, message)
	return nil
}

func (f *fakeMailSender) Events() []SlurmMailEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	events := make([]SlurmMailEvent, 0, len(f.messages))
	for _, message := range f.messages {
		events = append(events, message.Event)
	}
	return events
}

func TestSubmissionMailTypeMatchesSlurmEvents(t *testing.T) {
	cases := []struct {
		mailType string
		event    SlurmMailEvent
		want     bool
	}{
		{mailType: "NONE", event: SlurmMailBegin, want: false},
		{mailType: "", event: SlurmMailBegin, want: false},
		{mailType: "ALL", event: SlurmMailRequeue, want: true},
		{mailType: "BEGIN,END", event: SlurmMailBegin, want: true},
		{mailType: "BEGIN,END", event: SlurmMailFail, want: false},
		{mailType: "TIME_LIMIT", event: SlurmMailTimeLimit, want: true},
		{mailType: "FAIL", event: SlurmMailTimeLimit, want: true},
		{mailType: "REQUEUE", event: SlurmMailRequeue, want: true},
	}
	for _, tc := range cases {
		if got := submissionMailTypeMatches(tc.mailType, tc.event); got != tc.want {
			t.Fatalf("submissionMailTypeMatches(%q, %s) = %v, want %v", tc.mailType, tc.event, got, tc.want)
		}
	}
}

func TestBuildSlurmMailMessageContainsJobContext(t *testing.T) {
	sub := &models.Submission{
		ID:        "job-mail",
		UserID:    "u1",
		ProblemID: "p1",
		JobName:   "train",
		Cluster:   "debug",
		Status:    models.StatusFailed,
		Reason:    "TimeLimit",
	}
	message := buildSlurmMailMessage(sub, SlurmMailTimeLimit, "wall time exceeded", []string{"ops@example.com"})
	if message.Event != SlurmMailTimeLimit || message.JobID != sub.ID || len(message.To) != 1 || message.To[0] != "ops@example.com" {
		t.Fatalf("unexpected mail metadata: %#v", message)
	}
	for _, want := range []string{"JobID: job-mail", "JobName: train", "Event: TIME_LIMIT", "Reason: wall time exceeded"} {
		if !strings.Contains(message.Body, want) {
			t.Fatalf("mail body missing %q:\n%s", want, message.Body)
		}
	}
}
