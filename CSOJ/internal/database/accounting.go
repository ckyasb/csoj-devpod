package database

import (
	"fmt"
	"strings"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"gorm.io/gorm"
)

const (
	AccountEventSubmitted          = "Submitted"
	AccountEventStarted            = "Started"
	AccountEventCompleted          = "Completed"
	AccountEventFailed             = "Failed"
	AccountEventContainerStarted   = "ContainerStarted"
	AccountEventContainerFinished  = "ContainerFinished"
	AccountEventHeld               = "Held"
	AccountEventReleased           = "Released"
	AccountEventRequeued           = "Requeued"
	AccountEventInterrupted        = "Interrupted"
	AccountEventPreempted          = "Preempted"
	AccountEventSuspended          = "Suspended"
	AccountEventResumed            = "Resumed"
	AccountEventAllocated          = "Allocated"
	AccountEventAllocationReleased = "AllocationReleased"
	AccountEventRunStarted         = "RunStarted"
	AccountEventRunCompleted       = "RunCompleted"
	AccountEventRunFailed          = "RunFailed"
	AccountEventSignaled           = "Signaled"
)

func AccountingFromSubmission(sub *models.Submission, event string) models.AccountingRecord {
	if sub == nil {
		return models.AccountingRecord{Event: event}
	}
	return models.AccountingRecord{
		SubmissionID: sub.ID,
		UserID:       sub.UserID,
		ProblemID:    sub.ProblemID,
		JobName:      sub.JobName,
		Cluster:      sub.Cluster,
		Node:         sub.Node,
		Account:      sub.Account,
		QOS:          sub.QOS,
		ArrayJobID:   sub.ArrayJobID,
		ArrayTaskID:  sub.ArrayTaskID,
		Event:        event,
		State:        sub.Status,
		CPU:          sub.CPU,
		Memory:       sub.Memory,
		TRES:         sub.TRES,
		BillingUnits: sub.BillingUnits,
		Reason:       sub.Reason,
		Score:        sub.Score,
		Performance:  sub.Performance,
	}
}

func RecordAccounting(db *gorm.DB, record models.AccountingRecord) error {
	if db == nil {
		return fmt.Errorf("database is nil")
	}
	return db.Create(&record).Error
}

func GetAccountingRecords(db *gorm.DB, filters map[string]string, limit, offset int) ([]models.AccountingRecord, int64, error) {
	query, err := BuildAccountingRecordsQuery(db, filters)
	if err != nil {
		return nil, 0, err
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var records []models.AccountingRecord
	if err := query.Order("created_at DESC").Offset(offset).Limit(limit).Find(&records).Error; err != nil {
		return nil, 0, err
	}
	return records, total, nil
}

func BuildAccountingRecordsQuery(db *gorm.DB, filters map[string]string) (*gorm.DB, error) {
	query := db.Model(&models.AccountingRecord{})
	for key, value := range filters {
		if value == "" {
			continue
		}
		switch key {
		case "submission_id", "user_id", "problem_id", "cluster", "node", "account", "qos", "array_job_id", "array_task_id", "event", "state":
			values := splitAccountingFilterList(value)
			if len(values) > 0 {
				query = query.Where(key+" IN ?", values)
			}
		case "job_name":
			values := splitAccountingDelimitedFilterList(value)
			if len(values) > 0 {
				query = query.Where("job_name IN ? OR (job_name = '' AND problem_id IN ?)", values, values)
			}
		case "submission_ids":
			values := splitAccountingFilterList(value)
			if len(values) > 0 {
				query = query.Where("submission_id IN ?", values)
			}
		case "start_time":
			startTime, err := parseAccountingFilterTime(value)
			if err != nil {
				return nil, err
			}
			query = query.Where("created_at >= ?", startTime)
		case "end_time":
			endTime, err := parseAccountingFilterTime(value)
			if err != nil {
				return nil, err
			}
			query = query.Where("created_at <= ?", endTime)
		}
	}
	return query, nil
}

func splitAccountingFilterList(value string) []string {
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';' || r == ' '
	})
	out := make([]string, 0, len(fields))
	seen := make(map[string]bool, len(fields))
	for _, field := range fields {
		item := strings.TrimSpace(field)
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		out = append(out, item)
	}
	return out
}

func splitAccountingDelimitedFilterList(value string) []string {
	if strings.ContainsAny(value, ",;") {
		return splitAccountingFilterList(value)
	}
	item := strings.TrimSpace(value)
	if item == "" {
		return nil
	}
	return []string{item}
}

func parseAccountingFilterTime(value string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	var lastErr error
	for _, layout := range layouts {
		parsed, err := time.ParseInLocation(layout, value, time.Local)
		if err == nil {
			return parsed, nil
		}
		lastErr = err
	}
	return time.Time{}, fmt.Errorf("invalid accounting time %q: %w", value, lastErr)
}
