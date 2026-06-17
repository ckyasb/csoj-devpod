package admin

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
	devpodsvc "github.com/ZJUSCT/CSOJ/internal/devpod"
	"github.com/ZJUSCT/CSOJ/internal/util"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func (h *Handler) getAllDevPods(c *gin.Context) {
	sessions, err := database.GetAllDevPodSessions(h.db)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	for i := range sessions {
		if sessions[i].Status == models.DevPodStatusDeleted || sessions[i].Status == models.DevPodStatusDeleting {
			continue
		}
		if err := h.devpods.RefreshStatus(ctx, &sessions[i]); err == nil {
			_ = database.UpdateDevPodSession(h.db, &sessions[i])
		}
	}
	util.Success(c, adminDevPodResponses(sessions), "DevPods retrieved successfully")
}

func (h *Handler) getDevPod(c *gin.Context) {
	session, err := database.GetDevPodSession(h.db, c.Param("id"))
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			util.Error(c, http.StatusNotFound, "DevPod not found")
			return
		}
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	if session.Status != models.DevPodStatusDeleted {
		if err := h.devpods.RefreshStatus(ctx, session); err == nil {
			_ = database.UpdateDevPodSession(h.db, session)
		}
	}
	util.Success(c, adminDevPodResponse(*session), "DevPod retrieved successfully")
}

func (h *Handler) deleteDevPod(c *gin.Context) {
	session, err := database.GetDevPodSession(h.db, c.Param("id"))
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			util.Error(c, http.StatusNotFound, "DevPod not found")
			return
		}
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 20*time.Second)
	defer cancel()
	session.Status = models.DevPodStatusDeleting
	_ = database.UpdateDevPodSession(h.db, session)
	if err := h.devpods.DeleteDevPod(ctx, session); err != nil {
		session.Status = models.DevPodStatusFailed
		session.LastError = err.Error()
		_ = database.UpdateDevPodSession(h.db, session)
		h.recordAdminDevPodAudit(c, "admin_delete", session, "failed", err.Error())
		writeAdminDevPodError(c, err)
		return
	}
	session.Status = models.DevPodStatusDeleted
	session.LastError = ""
	if err := database.UpdateDevPodSession(h.db, session); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	h.recordAdminDevPodAudit(c, "admin_delete", session, "success", "")
	util.Success(c, adminDevPodResponse(*session), "DevPod deleted successfully")
}

func adminDevPodResponse(session models.DevPodSession) gin.H {
	return gin.H{
		"id":              session.ID,
		"userID":          session.UserID,
		"username":        session.Username,
		"ownerName":       session.OwnerName,
		"name":            session.Name,
		"displayName":     session.DisplayName,
		"image":           session.Image,
		"cpu":             session.CPU,
		"memoryMB":        session.MemoryMB,
		"gpu":             session.GPU,
		"storageGB":       session.StorageGB,
		"persistent":      session.Persistent,
		"networkProfile":  session.NetworkMode,
		"mpiEnabled":      session.MPIEnabled,
		"hostNetwork":     session.HostNetwork,
		"status":          session.Status,
		"namespace":       session.Namespace,
		"k8sResourceName": session.K8sResourceName,
		"sshCommand":      session.SSHCommand,
		"expiresAt":       session.ExpiresAt,
		"lastActivityAt":  session.LastActivityAt,
		"createdAt":       session.CreatedAt,
		"updatedAt":       session.UpdatedAt,
		"lastError":       session.LastError,
	}
}

func adminDevPodResponses(sessions []models.DevPodSession) []gin.H {
	out := make([]gin.H, 0, len(sessions))
	for _, session := range sessions {
		out = append(out, adminDevPodResponse(session))
	}
	return out
}

func writeAdminDevPodError(c *gin.Context, err error) {
	status, code, message := devpodsvc.ErrorResponse(err)
	c.JSON(status, gin.H{
		"code":    -1,
		"error":   code,
		"message": message,
		"details": gin.H{},
	})
}

func (h *Handler) recordAdminDevPodAudit(c *gin.Context, action string, session *models.DevPodSession, result, errorMessage string) {
	record := &models.DevPodAuditRecord{
		UserID:         session.UserID,
		Username:       session.Username,
		Action:         action,
		DevPodID:       session.ID,
		ResourceName:   session.K8sResourceName,
		Image:          session.Image,
		CPU:            session.CPU,
		MemoryMB:       session.MemoryMB,
		GPU:            session.GPU,
		NetworkProfile: session.NetworkMode,
		HostNetwork:    session.HostNetwork,
		MPIEnabled:     session.MPIEnabled,
		SourceIP:       c.ClientIP(),
		Result:         result,
		ErrorMessage:   errorMessage,
	}
	if err := database.RecordDevPodAudit(h.db, record); err != nil {
		zap.S().Warnf("failed to record admin DevPod audit event: %v", err)
	}
}
