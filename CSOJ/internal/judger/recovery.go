package judger

import (
	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// RecoverAndCleanup handles the recovery process on application startup.
// It finds submissions and containers that were in a 'Running' state
// and cleans up their associated runtime containers before marking them
// as 'Failed' in the database.
func RecoverAndCleanup(db *gorm.DB, cfg *config.Config) error {
	zap.S().Info("starting recovery process for interrupted submissions...")

	// 查找所有在运行时被中断的提交，并预加载它们关联的所有容器
	var interruptedSubs []models.Submission
	if err := db.Preload("Containers").Where("status IN ?", activeStatuses()).Find(&interruptedSubs).Error; err != nil {
		return err
	}

	if len(interruptedSubs) == 0 {
		zap.S().Info("no interrupted submissions found to recover")
		return nil
	}
	zap.S().Infof("found %d interrupted submissions to process", len(interruptedSubs))

	var submissionIDs []string

	for _, sub := range interruptedSubs {
		submissionIDs = append(submissionIDs, sub.ID)
		if sub.Cluster == "" || sub.Node == "" {
			zap.S().Warnf("submission %s has no cluster/node assigned, cannot clean up its containers", sub.ID)
			continue
		}

		if err := CleanupRuntimeContainers(cfg, sub.Cluster, sub.Node, sub.Containers); err != nil {
			zap.S().Errorf("failed to clean up runtime containers for submission %s on %s/%s: %v", sub.ID, sub.Cluster, sub.Node, err)
			continue
		}
	}

	// 清理完成后，在一个事务中更新数据库记录
	zap.S().Info("updating database status for interrupted submissions and containers")
	return db.Transaction(func(tx *gorm.DB) error {
		// 将被中断的提交标记为失败
		if err := tx.Model(&models.Submission{}).
			Where("id IN ?", submissionIDs).
			Updates(map[string]interface{}{
				"status": models.StatusFailed,
				"info":   models.JSONMap{"error": "System interrupted during execution"},
			}).Error; err != nil {
			return err
		}

		// 将关联的、正在运行的容器标记为失败
		if err := tx.Model(&models.Container{}).
			Where("submission_id IN ?", submissionIDs).
			Where("status IN ?", activeStatuses()).
			Update("status", models.StatusFailed).Error; err != nil {
			return err
		}
		return nil
	})
}
