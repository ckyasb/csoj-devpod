package judger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"github.com/ZJUSCT/CSOJ/internal/pubsub"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Dispatcher struct {
	cfg       *config.Config
	db        *gorm.DB
	scheduler *Scheduler
	appState  *AppState
}

type JudgeResult struct {
	Score       int                    `json:"score"`
	Performance float64                `json:"performance"`
	Info        map[string]interface{} `json:"info"`
}

type tempJudgeResult struct {
	Score       float64                `json:"score"`
	Performance float64                `json:"performance"`
	Info        map[string]interface{} `json:"info"`
}

func NewDispatcher(cfg *config.Config, db *gorm.DB, scheduler *Scheduler, appState *AppState) *Dispatcher {
	return &Dispatcher{
		cfg:       cfg,
		db:        db,
		scheduler: scheduler,
		appState:  appState,
	}
}

func (d *Dispatcher) Dispatch(sub *models.Submission, prob *Problem, node *NodeState, allocatedCores []int) {
	zap.S().Infof("dispatching submission %s to node %s", sub.ID, node.Name)

	runtimeFactory := NewRuntimeManager
	if d.scheduler != nil && d.scheduler.runtimeFactory != nil {
		runtimeFactory = d.scheduler.runtimeFactory
	}
	runtime, err := runtimeFactory(*node.Node)
	if err != nil {
		d.failSubmission(sub, prob, fmt.Sprintf("failed to create %s runtime client: %v", NodeRuntimeName(*node.Node), err))
		pubsub.GetBroker().CloseTopic(sub.ID)
		return
	}

	submissionVolumeName := sub.ID
	if err := runtime.CreateVolume(submissionVolumeName); err != nil {
		d.failSubmission(sub, prob, fmt.Sprintf("failed to create runtime work volume: %v", err))
		pubsub.GetBroker().CloseTopic(sub.ID)
		return
	}
	zap.S().Infof("created runtime work volume '%s' for submission %s", submissionVolumeName, sub.ID)

	defer func() {
		if err := runtime.RemoveVolume(submissionVolumeName); err != nil {
			zap.S().Errorf("failed to remove runtime work volume '%s': %v", submissionVolumeName, err)
		} else {
			zap.S().Infof("removed runtime work volume '%s' for submission %s", submissionVolumeName, sub.ID)
		}

		d.scheduler.ReleaseResources(prob.Cluster, sub.Node, allocatedCores, EffectiveMemory(prob, sub), sub.ID)
		zap.S().Infof("finished dispatching submission %s", sub.ID)
	}()

	var lastStdout string
	var coreStrs []string
	for _, c := range allocatedCores {
		coreStrs = append(coreStrs, strconv.Itoa(c))
	}
	cpusetCpus := strings.Join(coreStrs, ",")
	jobStart := time.Now()
	jobTimeLimit := explicitJobTimeLimit(sub, prob)
	batchIO, err := newBatchIORedirector(d.cfg.Storage.SubmissionContent, sub)
	if err != nil {
		d.failSubmission(sub, prob, fmt.Sprintf("failed to prepare batch I/O: %v", err))
		pubsub.GetBroker().CloseTopic(sub.ID)
		return
	}

	for i, flow := range prob.Workflow {
		stepTimeout, limitExceeded := workflowStepTimeout(flow.Timeout, jobStart, jobTimeLimit)
		if limitExceeded {
			d.failSubmission(sub, prob, "TimeLimit")
			pubsub.GetBroker().CloseTopic(sub.ID)
			return
		}
		sub.CurrentStep = i
		database.UpdateSubmission(d.db, sub)

		_, stdout, _, err := d.runWorkflowStep(runtime, sub, prob, flow, cpusetCpus, i, stepTimeout, batchIO)

		if err != nil {
			// runWorkflowStep cleans its own container; we just need to fail the submission.
			reason := fmt.Sprintf("workflow step %d failed: %v", i+1, err)
			if errors.Is(err, context.DeadlineExceeded) {
				reason = "TimeLimit"
			}
			d.failSubmission(sub, prob, reason)
			pubsub.GetBroker().CloseTopic(sub.ID)
			return // The main defer will handle volume and resource cleanup.
		}

		lastStdout = stdout
	}

	var tempResult tempJudgeResult
	if err := json.Unmarshal([]byte(lastStdout), &tempResult); err != nil {
		d.failSubmission(sub, prob, fmt.Sprintf("failed to parse judge result: %v. Raw output: %s", err, lastStdout))
		pubsub.GetBroker().CloseTopic(sub.ID)
		return
	}

	result := JudgeResult{
		Score:       int(math.Round((tempResult.Score))),
		Performance: tempResult.Performance,
		Info:        tempResult.Info,
	}

	contestID := d.findContestIDForProblem(prob.ID)
	if contestID == "" {
		zap.S().Warnf("cannot find contest for problem %s, skipping score update", prob.ID)
	}

	sub.Info = result.Info // common for both modes

	if prob.Score.Mode == "performance" && contestID != "" {
		sub.Performance = result.Performance
		// Score will be calculated by the DB function
		if err := database.UpdateScoresForPerformanceSubmission(d.db, sub, contestID, prob.Score.MaxPerformanceScore); err != nil {
			zap.S().Errorf("failed to update performance scores for submission %s: %v", sub.ID, err)
		}
		// After the transaction, the submission score in the DB is updated. Let's retrieve it to put it in the final object.
		var updatedSub models.Submission
		if errDb := d.db.Select("score").Where("id = ?", sub.ID).First(&updatedSub).Error; errDb == nil {
			sub.Score = updatedSub.Score
		} else {
			zap.S().Errorf("failed to retrieve updated score for submission %s: %v", sub.ID, errDb)
		}

	} else { // Default score mode or no contest found
		sub.Score = result.Score
		if contestID != "" {
			if err := database.UpdateScoresForNewSubmission(d.db, sub, contestID, sub.Score); err != nil {
				zap.S().Errorf("failed to update scores for submission %s: %v", sub.ID, err)
			}
		}
	}

	sub.Status = models.StatusSuccess
	if err := database.UpdateSubmission(d.db, sub); err != nil {
		zap.S().Errorf("failed to update successful submission %s: %v", sub.ID, err)
		return
	}
	record := database.AccountingFromSubmission(sub, database.AccountEventCompleted)
	record.CPU = schedulingCPUForSubmission(prob, sub)
	record.Memory = EffectiveMemory(prob, sub) * int64(batchNodeCount(sub))
	if err := database.RecordAccounting(d.db, record); err != nil {
		zap.S().Warnf("failed to record accounting completion event for submission %s: %v", sub.ID, err)
	}
	d.notifySubmissionMail(sub, SlurmMailEnd, "")

	zap.S().Infof("submission %s finished successfully with score %d", sub.ID, sub.Score)
	pubsub.GetBroker().CloseTopic(sub.ID)
}

func (d *Dispatcher) runWorkflowStep(runtime RuntimeManager, sub *models.Submission, prob *Problem, flow WorkflowStep, cpusetCpus string, step int, timeoutSeconds int, batchIO *batchIORedirector) (containerID, stdout, stderr string, err error) {
	zap.S().Debugf("Creating timeout context for step. Timeout: %d seconds", timeoutSeconds)
	stepCtx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	if err := os.MkdirAll(d.cfg.Storage.SubmissionLog, 0755); err != nil {
		return "", "", "", fmt.Errorf("failed to create log directory: %w", err)
	}
	logFileName := fmt.Sprintf("%s_%s.log", sub.ID, uuid.New().String())
	logFilePath := filepath.Join(d.cfg.Storage.SubmissionLog, logFileName)

	cont := &models.Container{
		ID:           uuid.New().String(),
		SubmissionID: sub.ID,
		UserID:       sub.UserID,
		Image:        flow.Image,
		Status:       models.StatusRunning,
		StartedAt:    time.Now(),
		LogFilePath:  logFilePath,
	}
	database.CreateContainer(d.db, cont)
	containerRecord := database.AccountingFromSubmission(sub, database.AccountEventContainerStarted)
	containerRecord.ContainerID = cont.ID
	containerRecord.StepName = flow.Name
	containerRecord.CPU = EffectiveCPU(prob, sub)
	containerRecord.Memory = EffectiveMemory(prob, sub)
	if err := database.RecordAccounting(d.db, containerRecord); err != nil {
		zap.S().Warnf("failed to record accounting container start event for %s: %v", cont.ID, err)
	}
	defer pubsub.GetBroker().CloseTopic(cont.ID)

	type result struct {
		ContainerID string
		Stdout      string
		Stderr      string
		Err         error
	}
	doneChan := make(chan result, 1)
	cidChan := make(chan string, 1)

	user, err := database.GetUserByID(d.db, sub.UserID)

	if err != nil {
		zap.S().Errorf("failed to get user %s: %v", sub.UserID, err)
		msg := pubsub.FormatMessage("error", fmt.Sprintf("Failed to fetch user: %v", err))
		d.failContainer(cont, -1, string(msg))
		cont.FinishedAt = time.Now()
		_ = database.UpdateContainer(d.db, cont)
		return "", "", "", fmt.Errorf("failed to get user: %w", err)
	}

	var containerEnvs = []string{
		"CSOJ_SUBMIT_DIR=/mnt/work",
		"CSOJ_USERNAME=" + user.Username,
	}
	containerEnvs = append(containerEnvs, batchSlurmEnv(sub, prob)...)
	if sub.ArrayJobID != "" {
		taskID := strconv.Itoa(sub.ArrayTaskID)
		containerEnvs = append(containerEnvs,
			"CSOJ_ARRAY_JOB_ID="+sub.ArrayJobID,
			"CSOJ_ARRAY_TASK_ID="+taskID,
			"CSOJ_ARRAY_TASK_COUNT="+strconv.Itoa(sub.ArrayTaskCount),
			"SLURM_ARRAY_JOB_ID="+sub.ArrayJobID,
			"SLURM_ARRAY_TASK_ID="+taskID,
			"SLURM_ARRAY_TASK_COUNT="+strconv.Itoa(sub.ArrayTaskCount),
		)
	}
	containerEnvs = append(containerEnvs, SubmissionEnvironmentVariables(sub)...)

	go func() {
		var execStdout, execStderr string
		var cid string
		var jsonLogBuffer bytes.Buffer // Buffer for NDJSON log file

		defer func() {
			if r := recover(); r != nil {
				zap.S().Errorf("Recovered from panic in dispatcher goroutine: %v", r)
				doneChan <- result{ContainerID: cid, Err: fmt.Errorf("panic recovered: %v", r)}
			}
		}()

		var containerName = sub.ID + "-" + strconv.Itoa(step)
		submissionVolumeName := sub.ID
		var err error
		cid, err = runtime.CreateContainer(flow.Image, submissionVolumeName, EffectiveCPU(prob, sub), cpusetCpus, EffectiveMemory(prob, sub), flow.Root, flow.Mounts, flow.Network, containerName, containerEnvs)
		if err != nil {
			logMsg := pubsub.FormatMessage("error", fmt.Sprintf("Failed to create container: %v", err))
			d.failContainer(cont, -1, string(logMsg)) // Set exit code to -1 for system errors

			doneChan <- result{Err: fmt.Errorf("failed to create container: %w", err)}
			return
		}
		zap.S().Infof("created container %s for submission %s step %d", cid, sub.ID, step)

		cidChan <- cid
		cont.DockerID = cid
		database.UpdateContainer(d.db, cont)

		if err := runtime.StartContainer(cid); err != nil {
			doneChan <- result{ContainerID: cid, Err: fmt.Errorf("failed to start container: %w", err)}
			return
		}

		if step == 0 {
			localWorkDir := filepath.Join(d.cfg.Storage.SubmissionContent, sub.ID)
			zap.S().Infof("copying files from %s to container %s:/mnt/work/", localWorkDir, cid)
			if err := runtime.CopyToContainer(cid, localWorkDir, "/mnt/work/"); err != nil {
				doneChan <- result{ContainerID: cid, Err: fmt.Errorf("failed to copy files to container: %w", err)}
				return
			}
		}

		for j, stepCmd := range flow.Steps {
			startMsg := pubsub.FormatMessage("info", fmt.Sprintf("\n--- Executing Command %d ---\n", j+1))
			jsonLogBuffer.Write(startMsg)
			jsonLogBuffer.WriteString("\n")
			pubsub.GetBroker().Publish(cont.ID, startMsg)

			outputCallback := func(streamType string, data []byte) {
				msg := pubsub.FormatMessage(streamType, string(data))
				pubsub.GetBroker().Publish(cont.ID, msg)
				jsonLogBuffer.Write(msg)
				jsonLogBuffer.WriteString("\n")
				if batchIO != nil {
					batchIO.Write(streamType, data)
				}
			}

			execCmd := batchCommandForSubmission(stepCmd, sub)
			execResult, err := runtime.ExecInContainer(stepCtx, cid, execCmd, outputCallback)

			exitMsg := pubsub.FormatMessage("info", fmt.Sprintf("\n--- Exit Code: %d ---\n", execResult.ExitCode))
			jsonLogBuffer.Write(exitMsg)
			jsonLogBuffer.WriteString("\n")
			pubsub.GetBroker().Publish(cont.ID, exitMsg)

			if err != nil || execResult.ExitCode != 0 {
				d.failContainer(cont, execResult.ExitCode, jsonLogBuffer.String())
				errMsg := fmt.Errorf("exec failed with exit code %d: %w", execResult.ExitCode, err)
				doneChan <- result{ContainerID: cid, Stdout: execResult.Stdout, Stderr: execResult.Stderr, Err: errMsg}
				return
			}
			execStdout = execResult.Stdout
			execStderr = execResult.Stderr
		}
		os.WriteFile(logFilePath, jsonLogBuffer.Bytes(), 0644)
		doneChan <- result{ContainerID: cid, Stdout: execStdout, Stderr: execStderr, Err: nil}
	}()

	var finalRes result
	var cidForCleanup string

	zap.S().Debugf("Entering select block for submission %s, waiting for completion or timeout...", sub.ID)
	select {
	case cidForCleanup = <-cidChan:
		select {
		case <-stepCtx.Done():
			zap.S().Warnf("TIMEOUT branch selected for submission %s. Cleaning up container %s.", sub.ID, cidForCleanup)
			runtime.CleanupContainer(cidForCleanup)
			d.failContainer(cont, -1, string(pubsub.FormatMessage("error", "Timeout exceeded")))
			return cidForCleanup, "", "Timeout exceeded", stepCtx.Err()

		case finalRes = <-doneChan:
			zap.S().Debugf("DONE_CHAN branch selected for submission %s. Error from goroutine: %v", sub.ID, finalRes.Err)
		}
	case <-stepCtx.Done():
		zap.S().Warnf("TIMEOUT branch selected for submission %s. Container was not even created.", sub.ID)
		d.failContainer(cont, -1, string(pubsub.FormatMessage("error", "Timeout exceeded before container creation")))
		return "", "", "Timeout exceeded", stepCtx.Err()

	case finalRes = <-doneChan:
		zap.S().Debugf("DONE_CHAN (early) branch selected for submission %s. Error from goroutine: %v", sub.ID, finalRes.Err)
	}

	// Always clean up the container if it was created, regardless of the outcome.
	if finalRes.ContainerID != "" {
		runtime.CleanupContainer(finalRes.ContainerID)
	}

	if finalRes.Err == nil {
		cont.Status = models.StatusSuccess
	}
	cont.FinishedAt = time.Now()
	database.UpdateContainer(d.db, cont)
	finishRecord := database.AccountingFromSubmission(sub, database.AccountEventContainerFinished)
	finishRecord.ContainerID = cont.ID
	finishRecord.StepName = flow.Name
	finishRecord.State = cont.Status
	finishRecord.ExitCode = cont.ExitCode
	finishRecord.CPU = EffectiveCPU(prob, sub)
	finishRecord.Memory = EffectiveMemory(prob, sub)
	if finalRes.Err != nil {
		finishRecord.Message = finalRes.Err.Error()
	}
	if err := database.RecordAccounting(d.db, finishRecord); err != nil {
		zap.S().Warnf("failed to record accounting container finish event for %s: %v", cont.ID, err)
	}
	return finalRes.ContainerID, finalRes.Stdout, finalRes.Stderr, finalRes.Err
}

func batchSlurmEnv(sub *models.Submission, prob *Problem) []string {
	if sub == nil {
		return nil
	}
	cpus := EffectiveCPU(prob, sub)
	ntasks := sub.NTasks
	if ntasks <= 0 {
		ntasks = 1
	}
	cpusPerTask := sub.CPUsPerTask
	if cpusPerTask <= 0 {
		cpusPerTask = cpus
		if ntasks > 0 && cpus > 0 && cpus%ntasks == 0 {
			cpusPerTask = cpus / ntasks
		}
		if cpusPerTask <= 0 {
			cpusPerTask = 1
		}
	}
	nodes := batchNodeCount(sub)
	cpusOnNode := batchSlurmPrimaryCPU(sub, cpus)
	return []string{
		"SLURM_JOB_ID=" + sub.ID,
		"SLURM_JOB_NAME=" + firstNonEmptyString(sub.JobName, sub.ProblemID),
		"SLURM_JOB_PARTITION=" + sub.Cluster,
		"SLURM_JOB_NODELIST=" + sub.Node,
		"SLURM_SUBMIT_DIR=/mnt/work",
		"SLURM_NTASKS=" + strconv.Itoa(ntasks),
		"SLURM_NPROCS=" + strconv.Itoa(ntasks),
		"SLURM_CPUS_PER_TASK=" + strconv.Itoa(cpusPerTask),
		"SLURM_CPUS_ON_NODE=" + strconv.Itoa(cpusOnNode),
		"SLURM_NNODES=" + strconv.Itoa(nodes),
		"SLURM_JOB_NUM_NODES=" + strconv.Itoa(nodes),
		"SLURM_JOB_CPUS_PER_NODE=" + batchSlurmCPUsPerNode(sub, cpus),
		"SLURM_MEM_PER_NODE=" + strconv.FormatInt(EffectiveMemory(prob, sub), 10),
	}
}

func batchNodeCount(sub *models.Submission) int {
	if sub != nil && sub.Nodes > 0 {
		return sub.Nodes
	}
	return 1
}

func batchSlurmPrimaryCPU(sub *models.Submission, fallbackCPU int) int {
	if sub != nil {
		if cores := parseAllocatedCores(sub.AllocatedCores); len(cores) > 0 {
			return len(cores)
		}
	}
	return fallbackCPU
}

func batchSlurmCPUsPerNode(sub *models.Submission, fallbackCPU int) string {
	if sub != nil && strings.TrimSpace(sub.AllocatedNodeCores) != "" {
		parts := strings.Split(sub.AllocatedNodeCores, ";")
		counts := make([]string, 0, len(parts))
		for _, part := range parts {
			_, cores, ok := strings.Cut(part, ":")
			if !ok {
				continue
			}
			counts = append(counts, strconv.Itoa(len(parseAllocatedCores(cores))))
		}
		if len(counts) > 0 {
			return strings.Join(counts, ",")
		}
	}
	return strconv.Itoa(fallbackCPU)
}

func explicitJobTimeLimit(sub *models.Submission, prob *Problem) int {
	if sub != nil && sub.TimeLimit > 0 {
		return sub.TimeLimit
	}
	if prob != nil && prob.Scheduling.TimeLimit > 0 {
		return prob.Scheduling.TimeLimit
	}
	return 0
}

func workflowStepTimeout(stepTimeout int, jobStart time.Time, jobTimeLimit int) (int, bool) {
	if jobTimeLimit <= 0 {
		return stepTimeout, false
	}
	remaining := time.Until(jobStart.Add(time.Duration(jobTimeLimit) * time.Second))
	if remaining <= 0 {
		return 0, true
	}
	remainingSeconds := int(math.Ceil(remaining.Seconds()))
	if stepTimeout <= 0 || remainingSeconds < stepTimeout {
		return remainingSeconds, false
	}
	return stepTimeout, false
}

type batchIORedirector struct {
	stdoutPath string
	stderrPath string
	mu         sync.Mutex
}

func newBatchIORedirector(submissionRoot string, sub *models.Submission) (*batchIORedirector, error) {
	redirector := &batchIORedirector{}
	if sub == nil || strings.TrimSpace(submissionRoot) == "" {
		return redirector, nil
	}

	stdoutSpec := strings.TrimSpace(sub.StdoutPath)
	stderrSpec := strings.TrimSpace(sub.StderrPath)
	if stdoutSpec == "" && stderrSpec == "" {
		return redirector, nil
	}
	openMode, err := normalizeBatchOpenMode(sub.OpenMode)
	if err != nil {
		return nil, err
	}

	basePath := filepath.Join(submissionRoot, sub.ID)
	if stdoutSpec != "" {
		redirector.stdoutPath, err = resolveBatchOutputPath(basePath, sub, stdoutSpec)
		if err != nil {
			return nil, fmt.Errorf("stdout_path: %w", err)
		}
	}
	if stderrSpec != "" {
		redirector.stderrPath, err = resolveBatchOutputPath(basePath, sub, stderrSpec)
		if err != nil {
			return nil, fmt.Errorf("stderr_path: %w", err)
		}
	} else if redirector.stdoutPath != "" {
		redirector.stderrPath = redirector.stdoutPath
	}

	for _, outputPath := range uniqueNonEmptyPaths(redirector.stdoutPath, redirector.stderrPath) {
		if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
			return nil, err
		}
		if openMode == "truncate" {
			if err := os.WriteFile(outputPath, nil, 0644); err != nil {
				return nil, err
			}
			continue
		}
		file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		if err := file.Close(); err != nil {
			return nil, err
		}
	}
	return redirector, nil
}

func (r *batchIORedirector) Write(streamType string, data []byte) {
	if r == nil || len(data) == 0 {
		return
	}
	outputPath := ""
	switch streamType {
	case "stdout":
		outputPath = r.stdoutPath
	case "stderr":
		outputPath = r.stderrPath
	default:
		return
	}
	if outputPath == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		zap.S().Warnf("failed to open batch %s file %s: %v", streamType, outputPath, err)
		return
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		zap.S().Warnf("failed to write batch %s file %s: %v", streamType, outputPath, err)
	}
}

func normalizeBatchOpenMode(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "truncate", "trunc":
		return "truncate", nil
	case "append":
		return "append", nil
	default:
		return "", fmt.Errorf("unsupported open_mode %q", mode)
	}
}

func resolveBatchOutputPath(basePath string, sub *models.Submission, pattern string) (string, error) {
	expanded := expandSlurmFilenamePattern(pattern, sub)
	if strings.TrimSpace(expanded) == "" {
		return "", fmt.Errorf("path is empty")
	}

	var target string
	if filepath.IsAbs(expanded) {
		rel, ok := containerPathToSubmissionRel(expanded)
		if !ok {
			return "", fmt.Errorf("absolute paths must be under /mnt/work")
		}
		target = filepath.Join(basePath, rel)
	} else {
		rel := filepath.Clean(filepath.FromSlash(expanded))
		if !isSafeRelativePath(rel) {
			return "", fmt.Errorf("path escapes submission directory")
		}
		if workRel, ok := batchWorkDirSubmissionRel(sub); ok && workRel != "" {
			rel = filepath.Join(workRel, rel)
		}
		target = filepath.Join(basePath, rel)
	}

	rel, err := filepath.Rel(basePath, target)
	if err != nil {
		return "", err
	}
	if !isSafeRelativePath(rel) {
		return "", fmt.Errorf("path escapes submission directory")
	}
	return target, nil
}

func expandSlurmFilenamePattern(pattern string, sub *models.Submission) string {
	var builder strings.Builder
	for i := 0; i < len(pattern); i++ {
		if pattern[i] != '%' || i+1 >= len(pattern) {
			builder.WriteByte(pattern[i])
			continue
		}
		i++
		switch pattern[i] {
		case '%':
			builder.WriteByte('%')
		case 'j':
			builder.WriteString(sub.ID)
		case 'A':
			builder.WriteString(firstNonEmptyString(sub.ArrayJobID, sub.ID))
		case 'a':
			if sub.ArrayJobID != "" {
				builder.WriteString(strconv.Itoa(sub.ArrayTaskID))
			} else {
				builder.WriteString("0")
			}
		case 'x':
			builder.WriteString(firstNonEmptyString(sub.JobName, sub.ProblemID, sub.ID))
		case 'u':
			builder.WriteString(sub.UserID)
		case 'N':
			builder.WriteString(sub.Node)
		default:
			builder.WriteByte('%')
			builder.WriteByte(pattern[i])
		}
	}
	return builder.String()
}

func batchWorkDirSubmissionRel(sub *models.Submission) (string, bool) {
	if sub == nil || strings.TrimSpace(sub.WorkDir) == "" {
		return "", true
	}
	workDir := filepath.ToSlash(filepath.Clean(strings.TrimSpace(sub.WorkDir)))
	if strings.HasPrefix(workDir, "/") {
		return containerPathToSubmissionRel(workDir)
	}
	rel := filepath.Clean(filepath.FromSlash(workDir))
	if !isSafeRelativePath(rel) {
		return "", false
	}
	return rel, true
}

func containerPathToSubmissionRel(containerPath string) (string, bool) {
	clean := path.Clean(filepath.ToSlash(containerPath))
	switch {
	case clean == "/mnt/work":
		return "", true
	case strings.HasPrefix(clean, "/mnt/work/"):
		rel := filepath.Clean(filepath.FromSlash(strings.TrimPrefix(clean, "/mnt/work/")))
		return rel, isSafeRelativePath(rel)
	default:
		return "", false
	}
}

func isSafeRelativePath(rel string) bool {
	clean := filepath.Clean(rel)
	return clean != "." && clean != ".." && !filepath.IsAbs(clean) && !strings.HasPrefix(clean, ".."+string(os.PathSeparator))
}

func uniqueNonEmptyPaths(paths ...string) []string {
	seen := map[string]struct{}{}
	unique := make([]string, 0, len(paths))
	for _, outputPath := range paths {
		if outputPath == "" {
			continue
		}
		if _, ok := seen[outputPath]; ok {
			continue
		}
		seen[outputPath] = struct{}{}
		unique = append(unique, outputPath)
	}
	return unique
}

func batchCommandForSubmission(cmd []string, sub *models.Submission) []string {
	if len(cmd) == 0 || sub == nil {
		return cmd
	}
	if strings.TrimSpace(sub.WorkDir) == "" && strings.TrimSpace(sub.StdinPath) == "" {
		return cmd
	}
	workDir := batchContainerWorkDir(sub.WorkDir)
	if stdinPath := batchContainerStdinPath(sub.StdinPath, workDir); stdinPath != "" {
		wrapped := []string{"/bin/sh", "-c", `cd "$1" && stdin="$2" && shift 2 && exec "$@" < "$stdin"`, "csoj-batch", workDir, stdinPath}
		return append(wrapped, cmd...)
	}
	wrapped := []string{"/bin/sh", "-c", `cd "$1" && shift && exec "$@"`, "csoj-batch", workDir}
	return append(wrapped, cmd...)
}

func batchContainerWorkDir(workDir string) string {
	workDir = strings.TrimSpace(workDir)
	if workDir == "" {
		return "/mnt/work"
	}
	clean := path.Clean(filepath.ToSlash(workDir))
	if strings.HasPrefix(clean, "/") {
		return clean
	}
	return path.Join("/mnt/work", clean)
}

func batchContainerStdinPath(stdinPath, workDir string) string {
	stdinPath = strings.TrimSpace(stdinPath)
	if stdinPath == "" {
		return ""
	}
	clean := path.Clean(filepath.ToSlash(stdinPath))
	if strings.HasPrefix(clean, "/") {
		return clean
	}
	return path.Join(workDir, clean)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func (d *Dispatcher) findContestIDForProblem(problemID string) string {
	d.appState.RLock()
	defer d.appState.RUnlock()
	if contest, ok := d.appState.ProblemToContestMap[problemID]; ok {
		return contest.ID
	}
	zap.S().Warnf("could not find parent contest for problem ID %s", problemID)
	return ""
}

func (d *Dispatcher) failSubmission(sub *models.Submission, prob *Problem, reason string) {
	zap.S().Errorf("submission %s failed: %s", sub.ID, reason)
	msg := pubsub.FormatMessage("error", reason)
	pubsub.GetBroker().Publish(sub.ID, msg)
	sub.Status = models.StatusFailed
	sub.Reason = reason
	sub.Info = map[string]interface{}{"error": reason}
	if err := database.UpdateSubmission(d.db, sub); err != nil {
		zap.S().Errorf("failed to update failed submission status for %s: %v", sub.ID, err)
	}
	record := database.AccountingFromSubmission(sub, database.AccountEventFailed)
	record.Message = reason
	record.CPU = schedulingCPUForSubmission(prob, sub)
	record.Memory = EffectiveMemory(prob, sub) * int64(batchNodeCount(sub))
	if err := database.RecordAccounting(d.db, record); err != nil {
		zap.S().Warnf("failed to record accounting failure event for submission %s: %v", sub.ID, err)
	}
	d.notifySubmissionMail(sub, slurmMailEventForFailure(reason), reason)
}

func (d *Dispatcher) notifySubmissionMail(sub *models.Submission, event SlurmMailEvent, detail string) {
	if d == nil || d.scheduler == nil {
		return
	}
	d.scheduler.notifySubmissionMail(sub, event, detail)
}

func (d *Dispatcher) failContainer(cont *models.Container, exitCode int, logContent string) {
	cont.Status = models.StatusFailed
	cont.ExitCode = exitCode
	cont.FinishedAt = time.Now()
	// On failure, write the log content to the file
	if err := os.WriteFile(cont.LogFilePath, []byte(logContent), 0644); err != nil {
		zap.S().Errorf("failed to write error log for container %s: %v", cont.ID, err)
	}
	database.UpdateContainer(d.db, cont)
}
