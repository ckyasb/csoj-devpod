package user

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
	devpodsvc "github.com/ZJUSCT/CSOJ/internal/devpod"
	"github.com/ZJUSCT/CSOJ/internal/util"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func (h *Handler) getDevPodOptions(c *gin.Context) {
	util.Success(c, h.devpodManager.Options(), "DevPod options retrieved successfully")
}

func (h *Handler) createDevPod(c *gin.Context) {
	userID := c.GetString("userID")
	user, err := database.GetUserByID(h.db, userID)
	if err != nil {
		util.Error(c, http.StatusNotFound, "user not found")
		return
	}

	var req devpodsvc.CreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeDevPodError(c, err)
		return
	}

	keys, err := database.GetUserSSHKeys(h.db, user.ID)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	openPods, err := database.CountOpenDevPodSessionsByUserID(h.db, user.ID)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	plan, err := h.devpodManager.ValidateCreateRequest(*user, keys, openPods, req)
	if err != nil {
		writeDevPodError(c, err)
		return
	}
	session, err := h.devpodManager.BuildSession(*user, plan)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	if err := database.CreateDevPodSession(h.db, &session); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	session.Status = models.DevPodStatusCreating
	if err := database.UpdateDevPodSession(h.db, &session); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	if err := h.devpodManager.SyncUser(ctx, *user, keys); err != nil {
		failDevPodSession(h.db, &session, err)
		h.recordDevPodAudit(c, "create", &session, "failed", err.Error())
		writeDevPodError(c, fmt.Errorf("sync devpod user: %w", err))
		return
	}
	if err := h.devpodManager.CreateDevPod(ctx, &session, plan); err != nil {
		failDevPodSession(h.db, &session, err)
		h.recordDevPodAudit(c, "create", &session, "failed", err.Error())
		writeDevPodError(c, fmt.Errorf("create devpod resource: %w", err))
		return
	}
	if err := h.devpodManager.RefreshStatus(ctx, &session); err == nil {
		_ = database.UpdateDevPodSession(h.db, &session)
	}
	h.recordDevPodAudit(c, "create", &session, "success", "")

	util.Success(c, devPodResponse(session), "DevPod creation requested successfully")
}

func (h *Handler) listDevPods(c *gin.Context) {
	userID := c.GetString("userID")
	sessions, err := database.GetDevPodSessionsByUserID(h.db, userID)
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
		if err := h.devpodManager.RefreshStatus(ctx, &sessions[i]); err == nil {
			_ = database.UpdateDevPodSession(h.db, &sessions[i])
		}
	}
	util.Success(c, devPodResponses(sessions), "DevPods retrieved successfully")
}

func (h *Handler) getDevPod(c *gin.Context) {
	session, ok := h.getOwnedDevPodSession(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	if session.Status != models.DevPodStatusDeleted {
		if err := h.devpodManager.RefreshStatus(ctx, session); err == nil {
			_ = database.UpdateDevPodSession(h.db, session)
		}
	}
	util.Success(c, devPodResponse(*session), "DevPod retrieved successfully")
}

func (h *Handler) stopDevPod(c *gin.Context) {
	session, ok := h.getOwnedDevPodSession(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()
	if err := h.devpodManager.SetRunning(ctx, session, false); err != nil {
		h.recordDevPodAudit(c, "stop", session, "failed", err.Error())
		writeDevPodError(c, err)
		return
	}
	session.Status = models.DevPodStatusStopped
	if err := database.UpdateDevPodSession(h.db, session); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	h.recordDevPodAudit(c, "stop", session, "success", "")
	util.Success(c, devPodResponse(*session), "DevPod stopped successfully")
}

func (h *Handler) startDevPod(c *gin.Context) {
	session, ok := h.getOwnedDevPodSession(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()
	if err := h.devpodManager.SetRunning(ctx, session, true); err != nil {
		h.recordDevPodAudit(c, "start", session, "failed", err.Error())
		writeDevPodError(c, err)
		return
	}
	session.Status = models.DevPodStatusCreating
	if err := database.UpdateDevPodSession(h.db, session); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	h.recordDevPodAudit(c, "start", session, "success", "")
	util.Success(c, devPodResponse(*session), "DevPod start requested successfully")
}

func (h *Handler) deleteDevPod(c *gin.Context) {
	session, ok := h.getOwnedDevPodSession(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 20*time.Second)
	defer cancel()
	session.Status = models.DevPodStatusDeleting
	_ = database.UpdateDevPodSession(h.db, session)
	if err := h.devpodManager.DeleteDevPod(ctx, session); err != nil {
		failDevPodSession(h.db, session, err)
		h.recordDevPodAudit(c, "delete", session, "failed", err.Error())
		writeDevPodError(c, err)
		return
	}
	session.Status = models.DevPodStatusDeleted
	session.LastError = ""
	if err := database.UpdateDevPodSession(h.db, session); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	h.recordDevPodAudit(c, "delete", session, "success", "")
	util.Success(c, devPodResponse(*session), "DevPod deleted successfully")
}

func (h *Handler) getDevPodSSH(c *gin.Context) {
	session, ok := h.getOwnedDevPodSession(c)
	if !ok {
		return
	}
	h.recordDevPodAudit(c, "ssh_info", session, "success", "")
	util.Success(c, gin.H{
		"sshCommand": session.SSHCommand,
		"sshHost":    session.SSHHost,
		"sshPort":    session.SSHPort,
		"sshUser":    session.SSHUser,
	}, "DevPod SSH info retrieved successfully")
}

func (h *Handler) getDevPodLogs(c *gin.Context) {
	session, ok := h.getOwnedDevPodSession(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	logs, err := h.devpodManager.Logs(ctx, session)
	if err != nil {
		writeDevPodError(c, err)
		return
	}
	util.Success(c, gin.H{"logs": logs}, "DevPod logs retrieved successfully")
}

func (h *Handler) listUserSSHKeys(c *gin.Context) {
	userID := c.GetString("userID")
	keys, err := database.GetUserSSHKeys(h.db, userID)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	util.Success(c, keys, "SSH keys retrieved successfully")
}

func (h *Handler) addUserSSHKey(c *gin.Context) {
	userID := c.GetString("userID")
	user, err := database.GetUserByID(h.db, userID)
	if err != nil {
		util.Error(c, http.StatusNotFound, "user not found")
		return
	}
	var req struct {
		Name      string `json:"name"`
		PublicKey string `json:"publicKey" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		writeDevPodError(c, err)
		return
	}
	publicKey, fingerprint, err := devpodsvc.ParseAuthorizedKey(req.PublicKey)
	if err != nil {
		writeDevPodError(c, err)
		return
	}
	if req.Name == "" {
		req.Name = fingerprint
	}
	key := models.UserSSHKey{
		ID:          uuid.NewString(),
		UserID:      user.ID,
		Name:        req.Name,
		PublicKey:   publicKey,
		Fingerprint: fingerprint,
	}
	if err := database.CreateUserSSHKey(h.db, &key); err != nil {
		writeDevPodError(c, fmt.Errorf("create SSH key: %w", err))
		return
	}
	keys, err := database.GetUserSSHKeys(h.db, user.ID)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()
	if err := h.devpodManager.SyncUser(ctx, *user, keys); err != nil {
		writeDevPodError(c, err)
		return
	}
	util.Success(c, key, "SSH key added successfully")
}

func (h *Handler) deleteUserSSHKey(c *gin.Context) {
	userID := c.GetString("userID")
	user, err := database.GetUserByID(h.db, userID)
	if err != nil {
		util.Error(c, http.StatusNotFound, "user not found")
		return
	}
	keyID := c.Param("id")
	if err := database.DeleteUserSSHKey(h.db, user.ID, keyID); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	keys, err := database.GetUserSSHKeys(h.db, user.ID)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()
	if err := h.devpodManager.SyncUser(ctx, *user, keys); err != nil {
		writeDevPodError(c, err)
		return
	}
	util.Success(c, nil, "SSH key deleted successfully")
}

func (h *Handler) getOwnedDevPodSession(c *gin.Context) (*models.DevPodSession, bool) {
	userID := c.GetString("userID")
	session, err := database.GetDevPodSessionForUser(h.db, c.Param("id"), userID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			util.Error(c, http.StatusNotFound, "DevPod not found")
			return nil, false
		}
		util.Error(c, http.StatusInternalServerError, err)
		return nil, false
	}
	return session, true
}

func devPodResponse(session models.DevPodSession) gin.H {
	return gin.H{
		"id":                 session.ID,
		"name":               session.Name,
		"displayName":        session.DisplayName,
		"image":              session.Image,
		"cpu":                session.CPU,
		"memoryMB":           session.MemoryMB,
		"gpu":                session.GPU,
		"storageGB":          session.StorageGB,
		"persistent":         session.Persistent,
		"networkProfile":     session.NetworkMode,
		"mpiEnabled":         session.MPIEnabled,
		"hostNetwork":        session.HostNetwork,
		"status":             session.Status,
		"namespace":          session.Namespace,
		"podName":            session.K8sResourceName,
		"k8sResourceName":    session.K8sResourceName,
		"sshCommand":         session.SSHCommand,
		"sshHost":            session.SSHHost,
		"sshPort":            session.SSHPort,
		"sshUser":            session.SSHUser,
		"expiresAt":          session.ExpiresAt,
		"lastActivityAt":     session.LastActivityAt,
		"createdAt":          session.CreatedAt,
		"updatedAt":          session.UpdatedAt,
		"idleTimeoutSeconds": session.IdleTimeoutSeconds,
		"lastError":          session.LastError,
	}
}

func devPodResponses(sessions []models.DevPodSession) []gin.H {
	out := make([]gin.H, 0, len(sessions))
	for _, session := range sessions {
		out = append(out, devPodResponse(session))
	}
	return out
}

func writeDevPodError(c *gin.Context, err error) {
	status, code, message := devpodsvc.ErrorResponse(err)
	c.JSON(status, gin.H{
		"code":    -1,
		"error":   code,
		"message": message,
		"details": gin.H{},
	})
}

func failDevPodSession(db *gorm.DB, session *models.DevPodSession, err error) {
	session.Status = models.DevPodStatusFailed
	session.LastError = err.Error()
	if updateErr := database.UpdateDevPodSession(db, session); updateErr != nil {
		zap.S().Warnf("failed to update failed DevPod session %s: %v", session.ID, updateErr)
	}
}

func (h *Handler) recordDevPodAudit(c *gin.Context, action string, session *models.DevPodSession, result, errorMessage string) {
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
		zap.S().Warnf("failed to record DevPod audit event: %v", err)
	}
}
