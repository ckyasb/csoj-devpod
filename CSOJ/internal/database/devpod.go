package database

import (
	"time"

	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"gorm.io/gorm"
)

var openDevPodStatuses = []models.DevPodStatus{
	models.DevPodStatusPending,
	models.DevPodStatusCreating,
	models.DevPodStatusRunning,
	models.DevPodStatusStopped,
}

func CreateDevPodSession(db *gorm.DB, session *models.DevPodSession) error {
	return db.Create(session).Error
}

func UpdateDevPodSession(db *gorm.DB, session *models.DevPodSession) error {
	return db.Save(session).Error
}

func GetDevPodSession(db *gorm.DB, id string) (*models.DevPodSession, error) {
	var session models.DevPodSession
	if err := db.Preload("User").Where("id = ?", id).First(&session).Error; err != nil {
		return nil, err
	}
	return &session, nil
}

func GetDevPodSessionForUser(db *gorm.DB, id, userID string) (*models.DevPodSession, error) {
	var session models.DevPodSession
	if err := db.Preload("User").Where("id = ? AND user_id = ?", id, userID).First(&session).Error; err != nil {
		return nil, err
	}
	return &session, nil
}

func GetDevPodSessionsByUserID(db *gorm.DB, userID string) ([]models.DevPodSession, error) {
	var sessions []models.DevPodSession
	if err := db.Where("user_id = ?", userID).Order("created_at desc").Find(&sessions).Error; err != nil {
		return nil, err
	}
	return sessions, nil
}

func GetAllDevPodSessions(db *gorm.DB) ([]models.DevPodSession, error) {
	var sessions []models.DevPodSession
	if err := db.Preload("User").Order("created_at desc").Find(&sessions).Error; err != nil {
		return nil, err
	}
	return sessions, nil
}

func CountOpenDevPodSessionsByUserID(db *gorm.DB, userID string) (int64, error) {
	var count int64
	err := db.Model(&models.DevPodSession{}).
		Where("user_id = ? AND status IN ?", userID, openDevPodStatuses).
		Count(&count).Error
	return count, err
}

func ExpireDevPodSessions(db *gorm.DB, now time.Time) error {
	return db.Model(&models.DevPodSession{}).
		Where("status IN ? AND expires_at <= ?", openDevPodStatuses, now).
		Update("status", models.DevPodStatusExpired).Error
}

func CreateUserSSHKey(db *gorm.DB, key *models.UserSSHKey) error {
	return db.Create(key).Error
}

func GetUserSSHKeys(db *gorm.DB, userID string) ([]models.UserSSHKey, error) {
	var keys []models.UserSSHKey
	if err := db.Where("user_id = ?", userID).Order("created_at asc").Find(&keys).Error; err != nil {
		return nil, err
	}
	return keys, nil
}

func DeleteUserSSHKey(db *gorm.DB, userID, keyID string) error {
	return db.Where("id = ? AND user_id = ?", keyID, userID).Delete(&models.UserSSHKey{}).Error
}

func RecordDevPodAudit(db *gorm.DB, record *models.DevPodAuditRecord) error {
	return db.Create(record).Error
}
