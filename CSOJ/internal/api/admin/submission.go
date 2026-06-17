package admin

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"github.com/ZJUSCT/CSOJ/internal/judger"
	"github.com/ZJUSCT/CSOJ/internal/pubsub"
	"github.com/ZJUSCT/CSOJ/internal/util"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func (h *Handler) getAllSubmissions(c *gin.Context) {
	// Pagination parameters
	pageStr := c.DefaultQuery("page", "1")
	limitStr := c.DefaultQuery("limit", "20")

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 20
	}
	if limit > 100 { // Add a reasonable upper bound for limit
		limit = 100
	}

	offset := (page - 1) * limit

	// Base query for filtering
	query := h.db.Model(&models.Submission{})

	if problemID := c.Query("problem_id"); problemID != "" {
		query = query.Where("submissions.problem_id = ?", problemID)
	}
	if status := c.Query("status"); status != "" {
		query = query.Where("submissions.status = ?", status)
	}
	if userQuery := c.Query("user_query"); userQuery != "" {
		likeQuery := "%" + userQuery + "%"
		// Join with users table to filter by user attributes
		query = query.Joins("JOIN users ON users.id = submissions.user_id").
			Where("users.id = ? OR users.username LIKE ? OR users.nickname LIKE ?", userQuery, likeQuery, likeQuery)
	}

	// Get total count
	var totalItems int64
	if err := query.Count(&totalItems).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	// Get paginated results
	var subs []models.Submission
	// We need to apply the same joins for the final query as for the count query
	// but the `query` variable already has them. We just need to add the preload and specify the table for ordering.
	if err := query.Preload("User").Order("submissions.created_at DESC").Offset(offset).Limit(limit).Find(&subs).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	models.PopulateSlurmStateForSubmissions(subs)

	totalPages := int(math.Ceil(float64(totalItems) / float64(limit)))

	response := gin.H{
		"items":        subs,
		"total_items":  totalItems,
		"total_pages":  totalPages,
		"current_page": page,
		"per_page":     limit,
	}

	util.Success(c, response, "Submissions retrieved successfully")
}

func (h *Handler) getSubmission(c *gin.Context) {
	sub, err := database.GetSubmission(h.db, c.Param("id"))
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	sub.PopulateSlurmState()
	util.Success(c, sub, "ok")
}

func (h *Handler) getSubmissionContent(c *gin.Context) {
	subID := c.Param("id")

	// Check if submission exists
	_, err := database.GetSubmission(h.db, subID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			util.Error(c, http.StatusNotFound, "submission not found")
		} else {
			util.Error(c, http.StatusInternalServerError, err)
		}
		return
	}

	submissionPath := filepath.Join(h.cfg.Storage.SubmissionContent, subID)

	// Check if the directory exists
	info, err := os.Stat(submissionPath)
	if os.IsNotExist(err) || !info.IsDir() {
		util.Error(c, http.StatusNotFound, "submission content not found on disk")
		return
	}

	// Create a buffer to write our archive to.
	buf := new(bytes.Buffer)
	zipWriter := zip.NewWriter(buf)

	// Walk the directory and add files to the zip.
	err = filepath.Walk(submissionPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Create a proper zip header
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}

		// Update the header name to be relative to the submission directory
		relPath, err := filepath.Rel(submissionPath, path)
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(relPath) // Use forward slashes in zip

		// If it's a directory, just create the header
		if info.IsDir() {
			header.Name += "/"
		} else {
			// Set compression method
			header.Method = zip.Deflate
		}

		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}

		// If it's a file, write its content to the zip
		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = io.Copy(writer, file)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		zap.S().Errorf("failed to create zip archive for submission %s: %v", subID, err)
		util.Error(c, http.StatusInternalServerError, "failed to create zip archive")
		return
	}

	// Close the zip writer to finalize the archive
	zipWriter.Close()

	// Set headers for file download
	c.Header("Content-Description", "File Transfer")
	c.Header("Content-Type", "application/zip")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"submission_%s.zip\"", subID))
	c.Data(http.StatusOK, "application/zip", buf.Bytes())
}

func (h *Handler) updateSubmission(c *gin.Context) {
	subID := c.Param("id")
	sub, err := database.GetSubmission(h.db, subID)
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}

	var req struct {
		Status          *models.Status  `json:"status"`
		Score           *int            `json:"score"`
		Performance     *float64        `json:"performance"`
		Info            *models.JSONMap `json:"info"`
		JobName         *string         `json:"job_name"`
		Name            *string         `json:"name"`
		WorkDir         *string         `json:"work_dir"`
		Chdir           *string         `json:"chdir"`
		StdinPath       *string         `json:"stdin_path"`
		Input           *string         `json:"input"`
		StdoutPath      *string         `json:"stdout_path"`
		Output          *string         `json:"output"`
		StderrPath      *string         `json:"stderr_path"`
		ErrorPath       *string         `json:"error"`
		OpenMode        *string         `json:"open_mode"`
		Comment         *string         `json:"comment"`
		MailType        *string         `json:"mail_type"`
		MailUser        *string         `json:"mail_user"`
		Exclusive       *bool           `json:"exclusive"`
		Requeue         *bool           `json:"requeue"`
		Export          *string         `json:"export"`
		Environment     *models.JSONMap `json:"environment"`
		Account         *string         `json:"account"`
		QOS             *string         `json:"qos"`
		Priority        *int            `json:"priority"`
		Nice            *int            `json:"nice"`
		Hold            *bool           `json:"hold"`
		CPU             *int            `json:"cpus"`
		NTasks          *int            `json:"ntasks"`
		CPUsPerTask     *int            `json:"cpus_per_task"`
		Nodes           *int            `json:"nodes"`
		Memory          *int64          `json:"memory"`
		BeginTime       *time.Time      `json:"begin_time"`
		Deadline        *time.Time      `json:"deadline"`
		TimeLimit       *int            `json:"time_limit"`
		Dependencies    *string         `json:"dependencies"`
		Reservation     *string         `json:"reservation"`
		NodeList        *string         `json:"nodelist"`
		ExcludeNodes    *string         `json:"exclude_nodes"`
		Constraint      *string         `json:"constraint"`
		GRES            *string         `json:"gres"`
		TRES            *string         `json:"tres"`
		Licenses        *string         `json:"licenses"`
		BillingUnits    *float64        `json:"billing_units"`
		Reason          *string         `json:"reason"`
		ArrayJobID      *string         `json:"array_job_id"`
		ArrayTaskID     *int            `json:"array_task_id"`
		ArraySpec       *string         `json:"array_spec"`
		ArrayTaskCount  *int            `json:"array_task_count"`
		ArrayMaxRunning *int            `json:"array_max_running"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}

	if req.Status != nil {
		sub.Status = *req.Status
	}
	if req.Score != nil {
		sub.Score = *req.Score
	}
	if req.Performance != nil {
		sub.Performance = *req.Performance
	}
	if req.Info != nil {
		sub.Info = *req.Info
	}
	if req.JobName != nil {
		sub.JobName = strings.TrimSpace(*req.JobName)
	} else if req.Name != nil {
		sub.JobName = strings.TrimSpace(*req.Name)
	}
	if req.WorkDir != nil {
		sub.WorkDir = strings.TrimSpace(*req.WorkDir)
	} else if req.Chdir != nil {
		sub.WorkDir = strings.TrimSpace(*req.Chdir)
	}
	if req.StdinPath != nil {
		sub.StdinPath = strings.TrimSpace(*req.StdinPath)
	} else if req.Input != nil {
		sub.StdinPath = strings.TrimSpace(*req.Input)
	}
	if req.StdoutPath != nil {
		sub.StdoutPath = strings.TrimSpace(*req.StdoutPath)
	} else if req.Output != nil {
		sub.StdoutPath = strings.TrimSpace(*req.Output)
	}
	if req.StderrPath != nil {
		sub.StderrPath = strings.TrimSpace(*req.StderrPath)
	} else if req.ErrorPath != nil {
		sub.StderrPath = strings.TrimSpace(*req.ErrorPath)
	}
	if req.OpenMode != nil {
		sub.OpenMode = strings.TrimSpace(*req.OpenMode)
	}
	if req.Comment != nil {
		sub.Comment = strings.TrimSpace(*req.Comment)
	}
	if req.MailType != nil {
		sub.MailType = strings.TrimSpace(*req.MailType)
	}
	if req.MailUser != nil {
		sub.MailUser = strings.TrimSpace(*req.MailUser)
	}
	if req.Exclusive != nil {
		sub.Exclusive = *req.Exclusive
	}
	if req.Requeue != nil {
		sub.Requeue = *req.Requeue
	}
	if req.Export != nil || req.Environment != nil {
		export := sub.ExportEnv
		if req.Export != nil {
			export = *req.Export
		}
		environment := sub.Environment
		if req.Environment != nil {
			environment = *req.Environment
		}
		parsedEnv, err := parseSlurmExportEnvironment(export, environment)
		if err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
		sub.ExportEnv = strings.TrimSpace(export)
		sub.Environment = parsedEnv
	}
	if req.Account != nil {
		sub.Account = *req.Account
	}
	if req.QOS != nil {
		sub.QOS = *req.QOS
	}
	if req.Priority != nil {
		sub.Priority = *req.Priority
	}
	if req.Nice != nil {
		sub.Nice = *req.Nice
	}
	if req.Hold != nil {
		sub.Hold = *req.Hold
	}
	if req.CPU != nil {
		if *req.CPU <= 0 {
			util.Error(c, http.StatusBadRequest, "cpus must be positive")
			return
		}
		sub.CPU = *req.CPU
	}
	if req.NTasks != nil {
		if *req.NTasks <= 0 {
			util.Error(c, http.StatusBadRequest, "ntasks must be positive")
			return
		}
		sub.NTasks = *req.NTasks
	}
	if req.CPUsPerTask != nil {
		if *req.CPUsPerTask <= 0 {
			util.Error(c, http.StatusBadRequest, "cpus_per_task must be positive")
			return
		}
		sub.CPUsPerTask = *req.CPUsPerTask
	}
	if req.Nodes != nil {
		if *req.Nodes <= 0 {
			util.Error(c, http.StatusBadRequest, "nodes must be positive")
			return
		}
		sub.Nodes = *req.Nodes
	}
	if req.CPU == nil && (req.NTasks != nil || req.CPUsPerTask != nil) {
		sub.CPU = slurmTotalCPUFromTaskShape(sub.NTasks, sub.CPUsPerTask)
	}
	if req.Memory != nil {
		if *req.Memory <= 0 {
			util.Error(c, http.StatusBadRequest, "memory must be positive")
			return
		}
		sub.Memory = *req.Memory
	}
	if req.BeginTime != nil {
		sub.BeginTime = req.BeginTime
	}
	if req.Deadline != nil {
		sub.Deadline = req.Deadline
	}
	if req.TimeLimit != nil {
		sub.TimeLimit = *req.TimeLimit
	}
	if req.Dependencies != nil {
		sub.Dependencies = *req.Dependencies
	}
	if req.Reservation != nil {
		sub.Reservation = *req.Reservation
	}
	if req.NodeList != nil {
		sub.NodeList = *req.NodeList
	}
	if req.ExcludeNodes != nil {
		sub.ExcludeNodes = *req.ExcludeNodes
	}
	if req.Constraint != nil {
		sub.Constraint = *req.Constraint
	}
	if req.GRES != nil {
		sub.GRES = *req.GRES
	}
	if req.TRES != nil {
		sub.TRES = *req.TRES
	}
	if req.Licenses != nil {
		if err := validateSlurmLicenses(*req.Licenses); err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
		sub.Licenses = strings.TrimSpace(*req.Licenses)
		sub.TRES = mergeSlurmLicensesIntoTRES(stripSlurmLicenseTRES(sub.TRES), sub.Licenses)
	}
	if req.BillingUnits != nil {
		sub.BillingUnits = *req.BillingUnits
	}
	if req.Reason != nil {
		sub.Reason = *req.Reason
	}
	if req.ArrayJobID != nil {
		sub.ArrayJobID = *req.ArrayJobID
	}
	if req.ArrayTaskID != nil {
		sub.ArrayTaskID = *req.ArrayTaskID
	}
	if req.ArraySpec != nil {
		sub.ArraySpec = *req.ArraySpec
	}
	if req.ArrayTaskCount != nil {
		sub.ArrayTaskCount = *req.ArrayTaskCount
	}
	if req.ArrayMaxRunning != nil {
		sub.ArrayMaxRunning = *req.ArrayMaxRunning
	}

	if err := database.UpdateSubmission(h.db, sub); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	zap.S().Warnf("admin manually updated submission %s", sub.ID)

	h.appState.RLock()
	contest, ok := h.appState.ProblemToContestMap[sub.ProblemID]
	problem, probOk := h.appState.Problems[sub.ProblemID]
	h.appState.RUnlock()
	if !ok || !probOk {
		zap.S().Errorf("failed to find parent contest or problem %s during score recalculation for submission %s", sub.ProblemID, sub.ID)
		sub.PopulateSlurmState()
		util.Success(c, sub, "Submission manually updated, but failed to trigger score recalculation: problem/contest definition not found.")
		return
	}

	if err := database.RecalculateScoresForUserProblem(h.db, sub.UserID, sub.ProblemID, contest.ID, sub.ID, problem.Score.Mode, problem.Score.MaxPerformanceScore); err != nil {
		util.Error(c, http.StatusInternalServerError, fmt.Errorf("submission manually updated, but failed to recalculate scores: %w", err))
		return
	}

	sub.PopulateSlurmState()
	util.Success(c, sub, "Submission manually updated and scores recalculated successfully.")
}

func (h *Handler) deleteSubmission(c *gin.Context) {
	subID := c.Param("id")
	// First, get submission to find its content path, if any.
	sub, err := database.GetSubmission(h.db, subID)
	if err != nil {
		util.Error(c, http.StatusNotFound, "submission not found")
		return
	}

	// Delete from DB. GORM's cascading delete will handle associated containers.
	if err := h.db.Delete(&models.Submission{}, subID).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, fmt.Errorf("failed to delete submission from database: %w", err))
		return
	}

	// Delete submission content from disk.
	submissionPath := filepath.Join(h.cfg.Storage.SubmissionContent, subID)
	if err := os.RemoveAll(submissionPath); err != nil {
		zap.S().Errorf("failed to delete submission content at %s: %v", submissionPath, err)
		util.Error(c, http.StatusInternalServerError, "DB record deleted, but failed to delete submission content from disk")
		return
	}
	zap.S().Warnf("admin deleted submission %s and its content", sub.ID)
	util.Success(c, nil, "Submission and its content deleted successfully")
}

func (h *Handler) getContainerLog(c *gin.Context) {
	con, err := database.GetContainer(h.db, c.Param("conID"))
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			util.Error(c, http.StatusNotFound, "Container not found")
			return
		}
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	if con.LogFilePath == "" {
		util.Error(c, http.StatusNotFound, "Log file path not recorded")
		return
	}

	file, err := os.Open(con.LogFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			util.Error(c, http.StatusNotFound, "Log file not found on disk")
			return
		}
		util.Error(c, http.StatusInternalServerError, "Failed to open log file")
		return
	}
	defer file.Close()

	c.Header("Content-Type", "application/x-ndjson; charset=utf-8")
	io.Copy(c.Writer, file)
}

func (h *Handler) rejudgeSubmission(c *gin.Context) {
	originalSubID := c.Param("id")
	originalSub, err := database.GetSubmission(h.db, originalSubID)
	if err != nil {
		util.Error(c, http.StatusNotFound, "Original submission not found")
		return
	}

	if err := database.UpdateSubmissionValidity(h.db, originalSub.ID, false); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	newSubID := uuid.NewString()
	newSub := models.Submission{
		ID:        newSubID,
		ProblemID: originalSub.ProblemID,
		UserID:    originalSub.UserID,
		Status:    models.StatusQueued,
		Cluster:   originalSub.Cluster,
		IsValid:   true,
	}
	judger.CopySubmissionScheduling(&newSub, originalSub)
	newSub.Reason = ""

	srcDir := filepath.Join(h.cfg.Storage.SubmissionContent, originalSub.ID)
	destDir := filepath.Join(h.cfg.Storage.SubmissionContent, newSubID)
	if err := copyDir(srcDir, destDir); err != nil {
		util.Error(c, http.StatusInternalServerError, fmt.Errorf("failed to copy submission content: %w", err))
		return
	}

	if err := database.CreateSubmission(h.db, &newSub); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	if err := database.RecordAccounting(h.db, database.AccountingFromSubmission(&newSub, database.AccountEventSubmitted)); err != nil {
		zap.S().Warnf("failed to record accounting submitted event for rejudge submission %s: %v", newSub.ID, err)
	}

	h.appState.RLock()
	problem, ok := h.appState.Problems[newSub.ProblemID]
	h.appState.RUnlock()
	if !ok {
		util.Error(c, http.StatusInternalServerError, "Problem definition not found for rejudge")
		return
	}
	h.scheduler.Submit(&newSub, problem)

	util.Success(c, gin.H{"new_submission_id": newSubID}, "Rejudge successfully submitted")
}

func (h *Handler) requeueSubmission(c *gin.Context) {
	subID := c.Param("id")
	sub, err := h.getSlurmJobBySelector(subID)
	if err != nil {
		util.Error(c, http.StatusNotFound, "Submission not found")
		return
	}
	if submissionActiveForAdmin(sub.Status) {
		util.Error(c, http.StatusBadRequest, "running submissions must be interrupted before requeue")
		return
	}

	h.appState.RLock()
	problem, ok := h.appState.Problems[sub.ProblemID]
	h.appState.RUnlock()
	if !ok {
		util.Error(c, http.StatusInternalServerError, "Problem definition not found for requeue")
		return
	}

	err = h.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("submission_id = ?", sub.ID).Delete(&models.Container{}).Error; err != nil {
			return err
		}
		return tx.Model(&models.Submission{}).Where("id = ?", sub.ID).Updates(map[string]interface{}{
			"status":          models.StatusQueued,
			"current_step":    0,
			"node":            "",
			"allocated_cores": "",
			"score":           0,
			"performance":     0,
			"info":            models.JSONMap{},
			"reason":          "",
		}).Error
	})
	if err != nil {
		util.Error(c, http.StatusInternalServerError, fmt.Errorf("failed to requeue submission: %w", err))
		return
	}

	sub.Status = models.StatusQueued
	sub.CurrentStep = 0
	sub.Node = ""
	sub.AllocatedCores = ""
	sub.Score = 0
	sub.Performance = 0
	sub.Info = models.JSONMap{}
	sub.Reason = ""
	if err := database.RecordAccounting(h.db, database.AccountingFromSubmission(sub, database.AccountEventRequeued)); err != nil {
		zap.S().Warnf("failed to record accounting requeue event for submission %s: %v", sub.ID, err)
	}
	h.scheduler.Submit(sub, problem)

	util.Success(c, sub, "Submission requeued")
}

func (h *Handler) updateSubmissionValidity(c *gin.Context) {
	subID := c.Param("id")
	var reqBody struct {
		IsValid bool `json:"is_valid"`
	}
	if err := c.ShouldBindJSON(&reqBody); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}

	// Get submission details BEFORE updating validity
	sub, err := database.GetSubmission(h.db, subID)
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}

	// First, apply the validity change to the submission
	if err := database.UpdateSubmissionValidity(h.db, subID, reqBody.IsValid); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	// Now, unconditionally trigger the score recalculation logic.
	// Get contest and problem info needed for the recalculation function.
	h.appState.RLock()
	contest, ok := h.appState.ProblemToContestMap[sub.ProblemID]
	problem, probOk := h.appState.Problems[sub.ProblemID]
	h.appState.RUnlock()
	if !ok || !probOk {
		// This should not happen in a consistent system, but handle it
		zap.S().Errorf("failed to find parent contest or problem %s during score recalculation for submission %s", sub.ProblemID, sub.ID)
		// Even if we can't find the problem definition, we proceed to send a success message because the validity itself was updated.
		// The error is logged for the admin to investigate.
		util.Success(c, nil, "Submission validity updated, but failed to trigger score recalculation: problem/contest definition not found.")
		return
	}

	// Trigger the comprehensive recalculation logic
	if err := database.RecalculateScoresForUserProblem(h.db, sub.UserID, sub.ProblemID, contest.ID, sub.ID, problem.Score.Mode, problem.Score.MaxPerformanceScore); err != nil {
		util.Error(c, http.StatusInternalServerError, fmt.Errorf("submission validity updated, but failed to recalculate scores: %w", err))
		return
	}

	util.Success(c, nil, "Submission validity updated and scores recalculated successfully.")
}

func (h *Handler) holdSubmission(c *gin.Context) {
	h.setSubmissionHold(c, true)
}

func (h *Handler) releaseSubmission(c *gin.Context) {
	h.setSubmissionHold(c, false)
}

func (h *Handler) setSubmissionHold(c *gin.Context, hold bool) {
	subID := c.Param("id")
	sub, err := h.getSlurmJobBySelector(subID)
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	if sub.Status != models.StatusQueued {
		util.Error(c, http.StatusBadRequest, "only queued submissions can be held or released")
		return
	}
	sub.Hold = hold
	if hold {
		sub.Reason = "JobHeld"
	} else if sub.Reason == "JobHeld" {
		sub.Reason = ""
	}
	if err := database.UpdateSubmission(h.db, sub); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	event := database.AccountEventReleased
	if hold {
		event = database.AccountEventHeld
	}
	if err := database.RecordAccounting(h.db, database.AccountingFromSubmission(sub, event)); err != nil {
		zap.S().Warnf("failed to record accounting hold event for submission %s: %v", sub.ID, err)
	}
	util.Success(c, sub, "Submission hold state updated")
}

func (h *Handler) interruptSubmission(c *gin.Context) {
	subID := c.Param("id")
	if sub, err := h.getSlurmJobBySelector(subID); err == nil {
		subID = sub.ID
	}
	message, err := h.interruptSubmissionByID(subID)
	if err != nil {
		writeAdminStatusError(c, err)
		return
	}
	util.Success(c, nil, message)
}

type adminStatusError struct {
	status  int
	message string
}

func (e adminStatusError) Error() string {
	return e.message
}

func newAdminStatusError(status int, message interface{}) error {
	return adminStatusError{status: status, message: fmt.Sprint(message)}
}

func writeAdminStatusError(c *gin.Context, err error) {
	if statusErr, ok := err.(adminStatusError); ok {
		util.Error(c, statusErr.status, statusErr.message)
		return
	}
	util.Error(c, http.StatusInternalServerError, err)
}

func (h *Handler) interruptSubmissionByID(subID string) (string, error) {
	sub, err := database.GetSubmission(h.db, subID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return "", newAdminStatusError(http.StatusNotFound, "Submission not found")
		}
		return "", newAdminStatusError(http.StatusInternalServerError, err)
	}

	switch sub.Status {
	case models.StatusQueued:
		sub.Status = models.StatusFailed
		sub.Reason = "Interrupted"
		sub.Info = models.JSONMap{"error": "Interrupted by admin while in queue"}
		if err := database.UpdateSubmission(h.db, sub); err != nil {
			return "", newAdminStatusError(http.StatusInternalServerError, fmt.Errorf("failed to update submission status: %w", err))
		}
		record := database.AccountingFromSubmission(sub, database.AccountEventInterrupted)
		record.Message = "Interrupted by admin while in queue"
		if err := database.RecordAccounting(h.db, record); err != nil {
			zap.S().Warnf("failed to record accounting interrupt event for submission %s: %v", sub.ID, err)
		}
		msg := pubsub.FormatMessage("error", "Submission interrupted by admin.")
		pubsub.GetBroker().Publish(sub.ID, msg)
		pubsub.GetBroker().CloseTopic(sub.ID)
		return "Queued submission interrupted", nil

	case models.StatusRunning, models.StatusSuspended:
		h.appState.RLock()
		problem, ok := h.appState.Problems[sub.ProblemID]
		h.appState.RUnlock()
		if !ok {
			return "", newAdminStatusError(http.StatusInternalServerError, "Problem definition not found for running submission")
		}

		if err := judger.CleanupRuntimeContainers(h.cfg, sub.Cluster, sub.Node, sub.Containers); err != nil {
			return "", newAdminStatusError(http.StatusInternalServerError, fmt.Errorf("failed to clean up runtime containers on node %s: %w", sub.Node, err))
		}

		err := h.db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&models.Submission{}).Where("id = ?", subID).Updates(map[string]interface{}{
				"status": models.StatusFailed,
				"reason": "Interrupted",
				"info":   models.JSONMap{"error": "Interrupted by admin while running"},
			}).Error; err != nil {
				return err
			}
			return tx.Model(&models.Container{}).Where("submission_id = ? AND status IN ?", subID, []models.Status{models.StatusRunning, models.StatusSuspended}).Update("status", models.StatusFailed).Error
		})
		if err != nil {
			return "", newAdminStatusError(http.StatusInternalServerError, fmt.Errorf("failed to update database: %w", err))
		}
		sub.Status = models.StatusFailed
		sub.Reason = "Interrupted"
		record := database.AccountingFromSubmission(sub, database.AccountEventInterrupted)
		record.Message = "Interrupted by admin while running"
		if err := database.RecordAccounting(h.db, record); err != nil {
			zap.S().Warnf("failed to record accounting interrupt event for submission %s: %v", sub.ID, err)
		}

		// Parse allocated cores from submission record to release them
		var coresToRelease []int
		if sub.AllocatedCores != "" {
			coreStrs := strings.Split(sub.AllocatedCores, ",")
			for _, s := range coreStrs {
				coreID, err := strconv.Atoi(s)
				if err == nil {
					coresToRelease = append(coresToRelease, coreID)
				}
			}
		}
		clusterName := sub.Cluster
		if clusterName == "" {
			clusterName = problem.Cluster
		}
		h.scheduler.ReleaseResources(clusterName, sub.Node, coresToRelease, judger.EffectiveMemory(problem, sub), sub.ID)

		msg := pubsub.FormatMessage("error", "Submission interrupted by admin.")
		pubsub.GetBroker().Publish(sub.ID, msg)
		pubsub.GetBroker().CloseTopic(sub.ID)
		return "Running submission interrupted successfully", nil

	case models.StatusSuccess, models.StatusFailed:
		return "", newAdminStatusError(http.StatusBadRequest, "Submission has already finished and cannot be interrupted")

	default:
		return "", newAdminStatusError(http.StatusInternalServerError, fmt.Sprintf("Unknown submission status: %s", sub.Status))
	}
}

func submissionActiveForAdmin(status models.Status) bool {
	return status == models.StatusRunning || status == models.StatusSuspended
}
