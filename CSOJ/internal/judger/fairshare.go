package judger

import (
	"math"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
)

func (s *Scheduler) fairshareUsagePenalty(accountName string, now time.Time) int {
	decay := s.cfg.Scheduler.FairshareDecay
	if !decay.Enabled || accountName == "" {
		return 0
	}
	usageWeight := decay.UsageWeight
	if usageWeight <= 0 {
		usageWeight = 1
	}
	shares := 1
	if account, ok := s.accountByName(accountName); ok && account.Fairshare > 0 {
		shares = account.Fairshare
	}
	usage := s.decayedAccountBillingUsage(accountName, now)
	return int(math.Round((usage / float64(shares)) * usageWeight))
}

func (s *Scheduler) decayedAccountBillingUsage(accountName string, now time.Time) float64 {
	halfLife := s.cfg.Scheduler.FairshareDecay.HalfLifeHours
	if halfLife <= 0 {
		halfLife = 24
	}
	var records []models.AccountingRecord
	err := s.db.
		Where("account = ? AND event IN ? AND billing_units > 0",
			accountName,
			[]string{
				database.AccountEventCompleted,
				database.AccountEventFailed,
				database.AccountEventPreempted,
				database.AccountEventInterrupted,
				database.AccountEventAllocationReleased,
			},
		).
		Find(&records).Error
	if err != nil {
		return 0
	}

	total := 0.0
	for _, record := range records {
		ageHours := now.Sub(record.CreatedAt).Hours()
		if ageHours < 0 {
			ageHours = 0
		}
		total += record.BillingUnits * math.Pow(0.5, ageHours/halfLife)
	}
	return total
}
