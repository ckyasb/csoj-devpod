package user

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
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

// containerResponse defines the structure for a container in a submission API response.
// It omits fields like image name and log file path for user-facing endpoints.
type containerResponse struct {
	ID         string        `json:"id"`
	CreatedAt  time.Time     `json:"CreatedAt"`
	UpdatedAt  time.Time     `json:"UpdatedAt"`
	Status     models.Status `json:"status"`
	ExitCode   int           `json:"exit_code"`
	StartedAt  time.Time     `json:"started_at"`
	FinishedAt time.Time     `json:"finished_at"`
}

// submissionResponse defines the structure for a submission API response, using containerResponse.
type submissionResponse struct {
	ID                 string              `json:"id"`
	CreatedAt          time.Time           `json:"CreatedAt"`
	UpdatedAt          time.Time           `json:"UpdatedAt"`
	ProblemID          string              `json:"problem_id"`
	UserID             string              `json:"user_id"`
	User               models.User         `json:"user"`
	Status             models.Status       `json:"status"`
	CurrentStep        int                 `json:"current_step"`
	JobName            string              `json:"job_name"`
	WorkDir            string              `json:"work_dir"`
	StdinPath          string              `json:"stdin_path"`
	StdoutPath         string              `json:"stdout_path"`
	StderrPath         string              `json:"stderr_path"`
	OpenMode           string              `json:"open_mode"`
	Comment            string              `json:"comment"`
	MailType           string              `json:"mail_type"`
	MailUser           string              `json:"mail_user"`
	Exclusive          bool                `json:"exclusive"`
	Requeue            bool                `json:"requeue"`
	Export             string              `json:"export"`
	Environment        models.JSONMap      `json:"environment"`
	Cluster            string              `json:"cluster"`
	Node               string              `json:"node"`
	AllocatedCores     string              `json:"allocated_cores"`
	AllocatedNodeCores string              `json:"allocated_node_cores"`
	NTasks             int                 `json:"ntasks"`
	CPUsPerTask        int                 `json:"cpus_per_task"`
	Nodes              int                 `json:"nodes"`
	Account            string              `json:"account"`
	QOS                string              `json:"qos"`
	Priority           int                 `json:"priority"`
	Nice               int                 `json:"nice"`
	Hold               bool                `json:"hold"`
	BeginTime          *time.Time          `json:"begin_time"`
	Deadline           *time.Time          `json:"deadline"`
	TimeLimit          int                 `json:"time_limit"`
	Dependencies       string              `json:"dependencies"`
	Reservation        string              `json:"reservation"`
	NodeList           string              `json:"nodelist"`
	ExcludeNodes       string              `json:"exclude_nodes"`
	Constraint         string              `json:"constraint"`
	GRES               string              `json:"gres"`
	TRES               string              `json:"tres"`
	Licenses           string              `json:"licenses"`
	BillingUnits       float64             `json:"billing_units"`
	Reason             string              `json:"reason"`
	SlurmState         string              `json:"slurm_state"`
	SlurmReason        string              `json:"slurm_reason"`
	ArrayJobID         string              `json:"array_job_id"`
	ArrayTaskID        int                 `json:"array_task_id"`
	ArraySpec          string              `json:"array_spec"`
	ArrayTaskCount     int                 `json:"array_task_count"`
	ArrayMaxRunning    int                 `json:"array_max_running"`
	Score              int                 `json:"score"`
	Performance        float64             `json:"performance"`
	Info               models.JSONMap      `json:"info"`
	IsValid            bool                `json:"is_valid"`
	Containers         []containerResponse `json:"containers"`
}

func (h *Handler) submitToProblem(c *gin.Context) {
	userID := c.GetString("userID")
	problemID := c.Param("id")

	user, err := database.GetUserByID(h.db, userID)
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}

	h.appState.RLock()
	problem, ok := h.appState.Problems[problemID]
	if !ok {
		h.appState.RUnlock()
		util.Error(c, http.StatusNotFound, fmt.Errorf("problem not found"))
		return
	}

	parentContest, ok := h.appState.ProblemToContestMap[problemID]
	if !ok {
		h.appState.RUnlock()
		util.Error(c, http.StatusInternalServerError, fmt.Errorf("internal server error: problem has no parent contest"))
		return
	}

	// Check if user is registered for the contest
	isRegistered, err := database.IsUserRegisteredForContest(h.db, user.ID, parentContest.ID)
	if err != nil {
		h.appState.RUnlock()
		util.Error(c, http.StatusInternalServerError, fmt.Errorf("failed to check contest registration: %w", err))
		return
	}
	if !isRegistered {
		h.appState.RUnlock()
		util.Error(c, http.StatusForbidden, fmt.Errorf("you must register for the contest before submitting"))
		return
	}

	// Check time restrictions for submission
	now := time.Now()
	if now.Before(parentContest.StartTime) || now.After(parentContest.EndTime) {
		h.appState.RUnlock()
		util.Error(c, http.StatusForbidden, fmt.Errorf("cannot submit because the contest is not active"))
		return
	}
	if now.Before(problem.StartTime) || now.After(problem.EndTime) {
		h.appState.RUnlock()
		util.Error(c, http.StatusForbidden, fmt.Errorf("cannot submit because the problem is not active"))
		return
	}
	h.appState.RUnlock()

	// Check submission limit
	if problem.MaxSubmissions > 0 {
		count, err := database.GetSubmissionCount(h.db, userID, parentContest.ID, problemID)
		if err != nil {
			util.Error(c, http.StatusInternalServerError, fmt.Errorf("failed to check submission count: %w", err))
			return
		}
		if count >= problem.MaxSubmissions {
			util.Error(c, http.StatusForbidden, fmt.Errorf("maximum submission limit of %d reached", problem.MaxSubmissions))
			return
		}
	}

	form, err := c.MultipartForm()
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	files := form.File["files"]

	if problem.Upload.MaxNum > 0 && len(files) > problem.Upload.MaxNum {
		msg := fmt.Sprintf("too many files uploaded. The maximum is %d, but you provided %d", problem.Upload.MaxNum, len(files))
		util.Error(c, http.StatusBadRequest, msg)
		return
	}

	if problem.Upload.MaxSize > 0 {
		var totalSize int64
		for _, file := range files {
			totalSize += file.Size
		}

		maxSizeBytes := int64(problem.Upload.MaxSize) * 1024 * 1024
		if totalSize > maxSizeBytes {
			msg := fmt.Sprintf("total file size exceeds the limit of %d MB", problem.Upload.MaxSize)
			util.Error(c, http.StatusRequestEntityTooLarge, msg)
			return
		}
	}

	arraySpec := firstFormValue(form.Value["array"])
	if arraySpec == "" {
		arraySpec = problem.Scheduling.Array
	}
	jobArray, err := judger.ParseJobArray(arraySpec)
	if err != nil {
		util.Error(c, http.StatusBadRequest, fmt.Errorf("invalid job array: %w", err))
		return
	}

	arrayJobID := uuid.New().String()
	submissionPath := filepath.Join(h.cfg.Storage.SubmissionContent, arrayJobID)
	if err := os.MkdirAll(submissionPath, 0755); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	for _, file := range files {
		rawBytes, err := base64.StdEncoding.DecodeString(file.Filename)
		var relativePath string
		if err == nil {
			relativePath = filepath.Clean(string(rawBytes))
		} else {
			util.Error(c, http.StatusBadRequest, fmt.Sprintf("failed to decode file path: %s", file.Filename))
		}

		// Backend validation against allowed file patterns from problem.yaml
		if len(problem.Upload.UploadFiles) > 0 {
			matched := false
			for _, pattern := range problem.Upload.UploadFiles {
				if m, _ := filepath.Match(pattern, relativePath); m {
					matched = true
					break
				}
			}
			if !matched {
				// Ban the user for 24 hours for submitting a disallowed file
				banUntil := time.Now().Add(24 * time.Hour)
				user.BannedUntil = &banUntil
				user.BanReason = "Hacking Detected"
				if err := database.UpdateUser(h.db, user); err != nil {
					util.Error(c, http.StatusInternalServerError, err)
					return
				}
				zap.S().Warnf("user %s (%s) auto-banned for 24 hours for uploading disallowed file: %s", user.Username, user.ID, relativePath)
				util.Error(c, http.StatusForbidden, "Your account has been temporarily banned due to suspicious activity.")
				return
			}
		}

		if filepath.IsAbs(relativePath) || strings.HasPrefix(relativePath, "..") {
			util.Error(c, http.StatusBadRequest, fmt.Sprintf("invalid file path: %s", file.Filename))
			return
		}

		dst := filepath.Join(submissionPath, relativePath)

		dst = filepath.Clean(dst)

		if !strings.HasPrefix(dst, submissionPath) {
			util.Error(c, http.StatusBadRequest, fmt.Sprintf("invalid file path after join: %s", file.Filename))
			return
		}

		if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			util.Error(c, http.StatusInternalServerError, fmt.Errorf("failed to create directory: %w", err))
			return
		}

		if err := c.SaveUploadedFile(file, dst); err != nil {
			util.Error(c, http.StatusInternalServerError, err)
			return
		}
	}

	taskIDs := jobArray.TaskIDs
	if len(taskIDs) == 0 {
		taskIDs = []int{0}
	}

	submissions := make([]models.Submission, 0, len(taskIDs))
	for i, taskID := range taskIDs {
		submissionID := arrayJobID
		if i > 0 {
			submissionID = uuid.NewString()
			if err := judger.CopyDir(submissionPath, filepath.Join(h.cfg.Storage.SubmissionContent, submissionID)); err != nil {
				os.RemoveAll(filepath.Join(h.cfg.Storage.SubmissionContent, submissionID))
				util.Error(c, http.StatusInternalServerError, fmt.Errorf("failed to prepare array task files: %w", err))
				return
			}
		}

		sub := models.Submission{
			ID:        submissionID,
			ProblemID: problemID,
			UserID:    user.ID,
			Status:    models.StatusQueued,
			Cluster:   problem.Cluster,
			IsValid:   true,
		}
		judger.ApplyProblemScheduling(&sub, problem)
		sub.BillingUnits = judger.CalculateBilling(h.cfg, problem, &sub)
		if jobArray.Spec != "" {
			sub.ArrayJobID = arrayJobID
			sub.ArrayTaskID = taskID
			sub.ArraySpec = jobArray.Spec
			sub.ArrayTaskCount = len(taskIDs)
			sub.ArrayMaxRunning = jobArray.MaxRunning
		}
		submissions = append(submissions, sub)
	}

	err = h.db.Transaction(func(tx *gorm.DB) error {
		for i := range submissions {
			if err := database.CreateSubmission(tx, &submissions[i]); err != nil {
				return err
			}
			record := database.AccountingFromSubmission(&submissions[i], database.AccountEventSubmitted)
			if err := database.RecordAccounting(tx, record); err != nil {
				return err
			}
		}
		if err := database.IncrementSubmissionCount(tx, user.ID, parentContest.ID, problemID); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		util.Error(c, http.StatusInternalServerError, fmt.Errorf("failed to create submission record: %w", err))
		return
	}

	submissionIDs := make([]string, 0, len(submissions))
	taskIDResponse := make([]int, 0, len(submissions))
	for i := range submissions {
		submissionIDs = append(submissionIDs, submissions[i].ID)
		taskIDResponse = append(taskIDResponse, submissions[i].ArrayTaskID)
		h.scheduler.Submit(&submissions[i], problem)
	}

	response := gin.H{"submission_id": submissionIDs[0]}
	if jobArray.Spec != "" {
		response["array_job_id"] = arrayJobID
		response["submission_ids"] = submissionIDs
		response["task_ids"] = taskIDResponse
		response["array_max_running"] = jobArray.MaxRunning
	}
	util.Success(c, response, "Submission received")
}

func firstFormValue(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (h *Handler) getProblemAttempts(c *gin.Context) {
	userID := c.GetString("userID")
	problemID := c.Param("id")

	h.appState.RLock()
	problem, ok := h.appState.Problems[problemID]
	if !ok {
		h.appState.RUnlock()
		util.Error(c, http.StatusNotFound, "problem not found")
		return
	}
	parentContest, ok := h.appState.ProblemToContestMap[problemID]
	if !ok {
		h.appState.RUnlock()
		util.Error(c, http.StatusInternalServerError, "internal server error: problem has no parent contest")
		return
	}
	h.appState.RUnlock()

	usedCount, err := database.GetSubmissionCount(h.db, userID, parentContest.ID, problemID)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, fmt.Errorf("failed to retrieve submission count: %w", err))
		return
	}

	type AttemptsResponse struct {
		Limit     *int `json:"limit"`
		Used      int  `json:"used"`
		Remaining *int `json:"remaining"`
	}

	resp := AttemptsResponse{Used: usedCount}

	if problem.MaxSubmissions > 0 {
		limit := problem.MaxSubmissions
		remaining := limit - usedCount
		if remaining < 0 {
			remaining = 0
		}
		resp.Limit = &limit
		resp.Remaining = &remaining
	}

	util.Success(c, resp, "Submission attempts retrieved successfully")
}

func (h *Handler) getUserSubmissions(c *gin.Context) {
	userID := c.GetString("userID")
	user, err := database.GetUserByID(h.db, userID)
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	subs, err := database.GetSubmissionsByUserID(h.db, user.ID)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	models.PopulateSlurmStateForSubmissions(subs)
	util.Success(c, subs, "ok")
}

func (h *Handler) getUserSubmission(c *gin.Context) {
	subID := c.Param("id")
	userID := c.GetString("userID")
	_, err := database.GetUserByID(h.db, userID)
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	sub, err := database.GetSubmission(h.db, subID)
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	if sub.UserID != userID {
		util.Error(c, http.StatusForbidden, fmt.Errorf("you can only view your own submissions"))
		return
	}

	// Build custom response to hide certain container fields
	respContainers := make([]containerResponse, len(sub.Containers))
	for i, cont := range sub.Containers {
		respContainers[i] = containerResponse{
			ID:         cont.ID,
			CreatedAt:  cont.CreatedAt,
			UpdatedAt:  cont.UpdatedAt,
			Status:     cont.Status,
			ExitCode:   cont.ExitCode,
			StartedAt:  cont.StartedAt,
			FinishedAt: cont.FinishedAt,
		}
	}

	slurmState, slurmReason := models.DeriveSlurmJobState(sub.Status, sub.Hold, sub.Reason)
	resp := submissionResponse{
		ID:                 sub.ID,
		CreatedAt:          sub.CreatedAt,
		UpdatedAt:          sub.UpdatedAt,
		ProblemID:          sub.ProblemID,
		UserID:             sub.UserID,
		User:               sub.User,
		Status:             sub.Status,
		CurrentStep:        sub.CurrentStep,
		JobName:            sub.JobName,
		WorkDir:            sub.WorkDir,
		StdinPath:          sub.StdinPath,
		StdoutPath:         sub.StdoutPath,
		StderrPath:         sub.StderrPath,
		OpenMode:           sub.OpenMode,
		Comment:            sub.Comment,
		MailType:           sub.MailType,
		MailUser:           sub.MailUser,
		Exclusive:          sub.Exclusive,
		Requeue:            sub.Requeue,
		Export:             sub.ExportEnv,
		Environment:        sub.Environment,
		Cluster:            sub.Cluster,
		Node:               sub.Node,
		AllocatedCores:     sub.AllocatedCores,
		AllocatedNodeCores: sub.AllocatedNodeCores,
		NTasks:             sub.NTasks,
		CPUsPerTask:        sub.CPUsPerTask,
		Nodes:              sub.Nodes,
		Account:            sub.Account,
		QOS:                sub.QOS,
		Priority:           sub.Priority,
		Nice:               sub.Nice,
		Hold:               sub.Hold,
		BeginTime:          sub.BeginTime,
		Deadline:           sub.Deadline,
		TimeLimit:          sub.TimeLimit,
		Dependencies:       sub.Dependencies,
		Reservation:        sub.Reservation,
		NodeList:           sub.NodeList,
		ExcludeNodes:       sub.ExcludeNodes,
		Constraint:         sub.Constraint,
		GRES:               sub.GRES,
		TRES:               sub.TRES,
		Licenses:           sub.Licenses,
		BillingUnits:       sub.BillingUnits,
		Reason:             sub.Reason,
		SlurmState:         slurmState,
		SlurmReason:        slurmReason,
		ArrayJobID:         sub.ArrayJobID,
		ArrayTaskID:        sub.ArrayTaskID,
		ArraySpec:          sub.ArraySpec,
		ArrayTaskCount:     sub.ArrayTaskCount,
		ArrayMaxRunning:    sub.ArrayMaxRunning,
		Score:              sub.Score,
		Performance:        sub.Performance,
		Info:               sub.Info,
		IsValid:            sub.IsValid,
		Containers:         respContainers,
	}
	util.Success(c, resp, "ok")
}

func (h *Handler) interruptSubmission(c *gin.Context) {
	subID := c.Param("id")
	userID := c.GetString("userID")
	user, err := database.GetUserByID(h.db, userID)
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}

	sub, err := database.GetSubmission(h.db, subID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			util.Error(c, http.StatusNotFound, "Submission not found")
			return
		}
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	// Authorization check
	if sub.UserID != user.ID {
		util.Error(c, http.StatusForbidden, "You can only interrupt your own submissions")
		return
	}

	switch sub.Status {
	case models.StatusQueued:
		sub.Status = models.StatusFailed
		sub.Reason = "Interrupted"
		sub.Info = models.JSONMap{"error": "Interrupted by user while in queue"}
		if err := database.UpdateSubmission(h.db, sub); err != nil {
			util.Error(c, http.StatusInternalServerError, fmt.Errorf("failed to update submission status: %w", err))
			return
		}
		record := database.AccountingFromSubmission(sub, database.AccountEventInterrupted)
		record.Message = "Interrupted by user while in queue"
		if err := database.RecordAccounting(h.db, record); err != nil {
			zap.S().Warnf("failed to record accounting interrupt event for submission %s: %v", sub.ID, err)
		}
		msg := pubsub.FormatMessage("error", "Submission interrupted by user.")
		pubsub.GetBroker().Publish(subID, msg)
		pubsub.GetBroker().CloseTopic(subID)
		util.Success(c, nil, "Queued submission interrupted")

	case models.StatusRunning, models.StatusSuspended:
		h.appState.RLock()
		problem, ok := h.appState.Problems[sub.ProblemID]
		h.appState.RUnlock()
		if !ok {
			util.Error(c, http.StatusInternalServerError, "Problem definition not found for running submission")
			return
		}

		if err := judger.CleanupRuntimeContainers(h.cfg, sub.Cluster, sub.Node, sub.Containers); err != nil {
			util.Error(c, http.StatusInternalServerError, fmt.Errorf("failed to clean up runtime containers on node %s: %w", sub.Node, err))
			return
		}

		err := h.db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&models.Submission{}).Where("id = ?", subID).Updates(map[string]interface{}{
				"status": models.StatusFailed,
				"reason": "Interrupted",
				"info":   models.JSONMap{"error": "Interrupted by user while running"},
			}).Error; err != nil {
				return err
			}
			return tx.Model(&models.Container{}).Where("submission_id = ? AND status IN ?", subID, []models.Status{models.StatusRunning, models.StatusSuspended}).Update("status", models.StatusFailed).Error
		})
		if err != nil {
			util.Error(c, http.StatusInternalServerError, fmt.Errorf("failed to update database: %w", err))
			return
		}
		sub.Status = models.StatusFailed
		sub.Reason = "Interrupted"
		record := database.AccountingFromSubmission(sub, database.AccountEventInterrupted)
		record.Message = "Interrupted by user while running"
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
		h.scheduler.ReleaseResources(problem.Cluster, sub.Node, coresToRelease, problem.Memory, sub.ID)

		msg := pubsub.FormatMessage("error", "Submission interrupted by user.")
		pubsub.GetBroker().Publish(subID, msg)
		pubsub.GetBroker().CloseTopic(subID)
		util.Success(c, nil, "Running submission interrupted successfully")

	case models.StatusSuccess, models.StatusFailed:
		util.Error(c, http.StatusBadRequest, "Submission has already finished and cannot be interrupted")

	default:
		util.Error(c, http.StatusInternalServerError, fmt.Sprintf("Unknown submission status: %s", sub.Status))
	}
}

func (h *Handler) getSubmissionQueuePosition(c *gin.Context) {
	subID := c.Param("id")
	userID := c.GetString("userID")

	user, err := database.GetUserByID(h.db, userID)
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}

	sub, err := database.GetSubmission(h.db, subID)
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}

	if sub.UserID != user.ID {
		util.Error(c, http.StatusForbidden, fmt.Errorf("you can only view your own submissions"))
		return
	}

	if sub.Status != models.StatusQueued {
		util.Success(c, gin.H{"position": 0}, "Submission is not in queue")
		return
	}

	count, err := database.CountQueuedSubmissionsBefore(h.db, sub.Cluster, sub.CreatedAt)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	util.Success(c, gin.H{"position": count}, "Queue position retrieved successfully")
}

func (h *Handler) getContainerLog(c *gin.Context) {
	subID := c.Param("id")
	conID := c.Param("conID")
	userID := c.GetString("userID")

	_, err := database.GetUserByID(h.db, userID)
	if err != nil {
		util.Error(c, http.StatusNotFound, "user not found")
		return
	}

	sub, err := database.GetSubmission(h.db, subID)
	if err != nil {
		util.Error(c, http.StatusNotFound, "submission not found")
		return
	}

	// Authorization Check : Ownership
	if sub.UserID != userID {
		util.Error(c, http.StatusForbidden, "you can only view your own submissions")
		return
	}

	var targetContainer *models.Container
	var containerIndex = -1
	// Sort containers by creation time to determine their step index
	sort.Slice(sub.Containers, func(i, j int) bool {
		return sub.Containers[i].CreatedAt.Before(sub.Containers[j].CreatedAt)
	})
	for i, c := range sub.Containers {
		if c.ID == conID {
			targetContainer = &sub.Containers[i]
			containerIndex = i
			break
		}
	}

	if targetContainer == nil {
		util.Error(c, http.StatusNotFound, "container not found in this submission")
		return
	}

	h.appState.RLock()
	problem, ok := h.appState.Problems[sub.ProblemID]
	h.appState.RUnlock()
	if !ok {
		util.Error(c, http.StatusInternalServerError, "problem definition not found")
		return
	}

	// Authorization Check : `show` flag in problem.yaml
	if containerIndex >= len(problem.Workflow) || !problem.Workflow[containerIndex].Show {
		util.Error(c, http.StatusForbidden, "you are not allowed to view the log for this step")
		return
	}

	file, err := os.Open(targetContainer.LogFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			util.Error(c, http.StatusNotFound, "log file not found on disk")
			return
		}
		util.Error(c, http.StatusInternalServerError, "failed to open log file")
		return
	}
	defer file.Close()

	c.Header("Content-Type", "application/x-ndjson; charset=utf-8")
	io.Copy(c.Writer, file)
}

func (h *Handler) getUserSubmissionContent(c *gin.Context) {
	subID := c.Param("id")
	userID := c.GetString("userID")

	_, err := database.GetUserByID(h.db, userID)
	if err != nil {
		util.Error(c, http.StatusNotFound, "user not found")
		return
	}

	sub, err := database.GetSubmission(h.db, subID)
	if err != nil {
		util.Error(c, http.StatusNotFound, "submission not found")
		return
	}

	// Authorization Check : Ownership
	if sub.UserID != userID {
		util.Error(c, http.StatusForbidden, "you can only download your own submissions")
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
