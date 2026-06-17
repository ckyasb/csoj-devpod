package database

import (
	"os"
	"path/filepath"

	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"go.uber.org/zap"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func Init(dsn string) (*gorm.DB, error) {
	if _, err := os.Stat(dsn); os.IsNotExist(err) {
		zap.S().Infof("database file not found at '%s', creating directory for it.", dsn)
		// Ensure the directory for the database file exists.
		dbDir := filepath.Dir(dsn)
		if err := os.MkdirAll(dbDir, 0755); err != nil {
			return nil, err
		}
	}

	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// Auto migrate schema
	err = db.AutoMigrate(
		&models.User{},
		&models.UserSSHKey{},
		&models.Submission{},
		&models.Container{},
		&models.Allocation{},
		&models.RunStep{},
		&models.AccountingRecord{},
		&models.DevPodSession{},
		&models.DevPodAuditRecord{},
		&models.SlurmTrigger{},
		&models.SlurmCronJob{},
		&models.ContestScoreHistory{},
		&models.UserProblemBestScore{},
	)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func RecoverInterrupted(db *gorm.DB) error {
	// Mark running submissions as failed
	result := db.Model(&models.Submission{}).
		Where("status IN ?", []models.Status{models.StatusRunning, models.StatusSuspended}).
		Updates(map[string]interface{}{
			"status": models.StatusFailed,
			"info":   "System interrupted",
		})
	if result.Error != nil {
		return result.Error
	}

	// Mark running containers as failed
	result = db.Model(&models.Container{}).
		Where("status IN ?", []models.Status{models.StatusRunning, models.StatusSuspended}).
		Updates(map[string]interface{}{
			"status": models.StatusFailed,
		})
	if result.Error != nil {
		return result.Error
	}

	return nil
}
