package database

import (
	"testing"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestAccountingRecordsCanBeRecordedAndFiltered(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&models.AccountingRecord{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	records := []models.AccountingRecord{
		{CreatedAt: now.Add(-2 * time.Hour), SubmissionID: "s1", UserID: "u1", ProblemID: "p1", Event: AccountEventSubmitted, State: models.StatusQueued, QOS: "normal"},
		{CreatedAt: now.Add(-1 * time.Hour), SubmissionID: "s1", UserID: "u1", ProblemID: "p1", Event: AccountEventStarted, State: models.StatusRunning, QOS: "normal"},
		{CreatedAt: now, SubmissionID: "s2", UserID: "u2", ProblemID: "p1", Event: AccountEventSubmitted, State: models.StatusQueued, QOS: "urgent"},
	}
	for _, record := range records {
		if err := RecordAccounting(db, record); err != nil {
			t.Fatalf("record accounting: %v", err)
		}
	}

	filtered, total, err := GetAccountingRecords(db, map[string]string{
		"submission_id": "s1",
	}, 20, 0)
	if err != nil {
		t.Fatalf("get accounting: %v", err)
	}
	if total != 2 || len(filtered) != 2 {
		t.Fatalf("expected two records for s1, got total=%d len=%d", total, len(filtered))
	}

	filtered, total, err = GetAccountingRecords(db, map[string]string{
		"qos": "debug,urgent",
	}, 20, 0)
	if err != nil {
		t.Fatalf("get accounting by qos: %v", err)
	}
	if total != 1 || len(filtered) != 1 || filtered[0].SubmissionID != "s2" {
		t.Fatalf("unexpected urgent records: total=%d records=%v", total, filtered)
	}

	filtered, total, err = GetAccountingRecords(db, map[string]string{
		"user_id": "u0 u2",
		"event":   AccountEventSubmitted + "," + AccountEventCompleted,
	}, 20, 0)
	if err != nil {
		t.Fatalf("get accounting by list filters: %v", err)
	}
	if total != 1 || len(filtered) != 1 || filtered[0].SubmissionID != "s2" {
		t.Fatalf("unexpected list-filtered records: total=%d records=%v", total, filtered)
	}

	filtered, total, err = GetAccountingRecords(db, map[string]string{
		"submission_ids": "s1,s2,s1",
	}, 20, 0)
	if err != nil {
		t.Fatalf("get accounting by submission ids: %v", err)
	}
	if total != 3 || len(filtered) != 3 {
		t.Fatalf("expected all records for s1/s2, got total=%d len=%d", total, len(filtered))
	}

	filtered, total, err = GetAccountingRecords(db, map[string]string{
		"start_time": now.Add(-90 * time.Minute).Format(time.RFC3339),
		"end_time":   now.Add(-30 * time.Minute).Format(time.RFC3339),
	}, 20, 0)
	if err != nil {
		t.Fatalf("get accounting by time range: %v", err)
	}
	if total != 1 || len(filtered) != 1 || filtered[0].Event != AccountEventStarted {
		t.Fatalf("unexpected time-filtered records: total=%d records=%v", total, filtered)
	}
}
