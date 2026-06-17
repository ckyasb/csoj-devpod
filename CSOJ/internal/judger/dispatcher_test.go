package judger

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"gorm.io/gorm"
)

type fakeBatchRuntime struct {
	execDeadlines []time.Duration
	execCommands  [][]string
	createdEnvs   [][]string
	stdout        string
	stderr        string
}

func (f *fakeBatchRuntime) CreateVolume(name string) error { return nil }
func (f *fakeBatchRuntime) RemoveVolume(name string) error { return nil }
func (f *fakeBatchRuntime) CreateContainer(image, volumeName string, cpu int, cpusetCpus string, memory int64, asRoot bool, customMounts []Mount, networkEnabled bool, name string, envs []string) (string, error) {
	f.createdEnvs = append(f.createdEnvs, append([]string(nil), envs...))
	return "container-" + name, nil
}
func (f *fakeBatchRuntime) StartContainer(containerID string) error { return nil }
func (f *fakeBatchRuntime) ExecInContainer(ctx context.Context, containerID string, cmd []string, outputCallback func(streamType string, data []byte)) (ExecResult, error) {
	if deadline, ok := ctx.Deadline(); ok {
		f.execDeadlines = append(f.execDeadlines, time.Until(deadline))
	}
	f.execCommands = append(f.execCommands, append([]string(nil), cmd...))
	stdout := f.stdout
	if stdout == "" {
		stdout = `{"score":100,"performance":1,"info":{}}`
	}
	if outputCallback != nil {
		outputCallback("stdout", []byte(stdout))
		if f.stderr != "" {
			outputCallback("stderr", []byte(f.stderr))
		}
	}
	return ExecResult{Stdout: stdout, Stderr: f.stderr, ExitCode: 0}, nil
}
func (f *fakeBatchRuntime) PauseContainer(containerID string) error                 { return nil }
func (f *fakeBatchRuntime) ResumeContainer(containerID string) error                { return nil }
func (f *fakeBatchRuntime) SignalContainer(containerID string, signal string) error { return nil }
func (f *fakeBatchRuntime) CleanupContainer(containerID string)                     {}
func (f *fakeBatchRuntime) CopyToContainer(containerID string, srcDir string, dstDir string) error {
	return nil
}

func TestDispatcherMirrorsBatchIOAndWrapsWorkDirStdin(t *testing.T) {
	contentDir := t.TempDir()
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 1, Memory: 128}},
		}},
		Storage: config.Storage{
			SubmissionContent: contentDir,
			SubmissionLog:     t.TempDir(),
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	runtime := &fakeBatchRuntime{
		stdout: `{"score":77,"performance":1,"info":{"ok":true}}`,
		stderr: "warning\n",
	}
	scheduler.SetRuntimeFactory(func(config.Node) (RuntimeManager, error) {
		return runtime, nil
	})
	user := models.User{ID: "u1", Username: "alice"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	submissionID := "job-io"
	workPath := filepath.Join(contentDir, submissionID, "work")
	if err := os.MkdirAll(filepath.Join(workPath, "logs"), 0755); err != nil {
		t.Fatalf("create work dir: %v", err)
	}
	outPath := filepath.Join(workPath, "logs", submissionID+".out")
	errPath := filepath.Join(workPath, "logs", submissionID+".err")
	if err := os.WriteFile(outPath, []byte("old stdout\n"), 0644); err != nil {
		t.Fatalf("seed stdout: %v", err)
	}
	if err := os.WriteFile(errPath, []byte("old stderr\n"), 0644); err != nil {
		t.Fatalf("seed stderr: %v", err)
	}

	problem := &Problem{
		ID:      "p1",
		Cluster: "debug",
		CPU:     1,
		Memory:  128,
		Workflow: []WorkflowStep{{
			Name:    "judge",
			Image:   "runner",
			Timeout: 60,
			Steps:   [][]string{{"judge", "--case", "1"}},
		}},
	}
	submission := &models.Submission{
		ID:             submissionID,
		UserID:         user.ID,
		ProblemID:      problem.ID,
		Status:         models.StatusRunning,
		Cluster:        "debug",
		Node:           "n1",
		AllocatedCores: "0",
		JobName:        "io-test",
		WorkDir:        "work",
		StdinPath:      "input.txt",
		StdoutPath:     "logs/%j.out",
		StderrPath:     "logs/%j.err",
		OpenMode:       "append",
	}
	if err := db.Create(submission).Error; err != nil {
		t.Fatalf("create submission: %v", err)
	}

	scheduler.dispatcher.Dispatch(submission, problem, scheduler.clusters["debug"].Nodes["n1"], []int{0})

	if len(runtime.execCommands) != 1 {
		t.Fatalf("expected one exec command, got %d", len(runtime.execCommands))
	}
	cmd := runtime.execCommands[0]
	if len(cmd) < 8 || cmd[0] != "/bin/sh" || cmd[3] != "csoj-batch" || cmd[4] != "/mnt/work/work" || cmd[5] != "/mnt/work/work/input.txt" || cmd[6] != "judge" {
		t.Fatalf("unexpected wrapped command: %#v", cmd)
	}
	stdoutData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read stdout file: %v", err)
	}
	if string(stdoutData) != "old stdout\n"+runtime.stdout {
		t.Fatalf("unexpected stdout file %q", string(stdoutData))
	}
	stderrData, err := os.ReadFile(errPath)
	if err != nil {
		t.Fatalf("read stderr file: %v", err)
	}
	if string(stderrData) != "old stderr\n"+runtime.stderr {
		t.Fatalf("unexpected stderr file %q", string(stderrData))
	}

	var updated models.Submission
	if err := db.First(&updated, "id = ?", submission.ID).Error; err != nil {
		t.Fatalf("load updated submission: %v", err)
	}
	if updated.Status != models.StatusSuccess || updated.Score != 77 {
		t.Fatalf("submission should finish through fake runtime, got status=%s score=%d", updated.Status, updated.Score)
	}
}

func TestDispatcherUsesRuntimeFactoryAndJobTimeLimit(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 1, Memory: 128}},
		}},
		Storage: config.Storage{
			SubmissionContent: t.TempDir(),
			SubmissionLog:     t.TempDir(),
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	runtime := &fakeBatchRuntime{}
	scheduler.SetRuntimeFactory(func(config.Node) (RuntimeManager, error) {
		return runtime, nil
	})
	user := models.User{ID: "u1", Username: "u1"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	problem := &Problem{
		ID:      "p1",
		Cluster: "debug",
		CPU:     1,
		Memory:  128,
		Workflow: []WorkflowStep{{
			Name:    "judge",
			Image:   "runner",
			Timeout: 60,
			Steps:   [][]string{{"judge"}},
		}},
	}
	submission := &models.Submission{
		ID:             "job-time-limit",
		UserID:         user.ID,
		ProblemID:      problem.ID,
		Status:         models.StatusRunning,
		Cluster:        "debug",
		Node:           "n1",
		AllocatedCores: "0",
		TimeLimit:      1,
	}
	if err := db.Create(submission).Error; err != nil {
		t.Fatalf("create submission: %v", err)
	}

	scheduler.dispatcher.Dispatch(submission, problem, scheduler.clusters["debug"].Nodes["n1"], []int{0})

	if len(runtime.execDeadlines) != 1 {
		t.Fatalf("expected one runtime exec with injected runtime, got %d", len(runtime.execDeadlines))
	}
	if runtime.execDeadlines[0] <= 0 || runtime.execDeadlines[0] > 1500*time.Millisecond {
		t.Fatalf("job time limit should cap step deadline near 1s, got %s", runtime.execDeadlines[0])
	}
	var updated models.Submission
	if err := db.First(&updated, "id = ?", submission.ID).Error; err != nil {
		t.Fatalf("load updated submission: %v", err)
	}
	if updated.Status != models.StatusSuccess || updated.Score != 100 {
		t.Fatalf("submission should finish through fake runtime, got status=%s score=%d", updated.Status, updated.Score)
	}
}

func TestSchedulerAndDispatcherSendSlurmMailEvents(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 1, Memory: 128}},
		}},
		Storage: config.Storage{
			SubmissionContent: t.TempDir(),
			SubmissionLog:     t.TempDir(),
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	runtime := &fakeBatchRuntime{}
	scheduler.SetRuntimeFactory(func(config.Node) (RuntimeManager, error) {
		return runtime, nil
	})
	mailer := &fakeMailSender{}
	scheduler.SetMailSender(mailer)
	user := models.User{ID: "u1", Username: "u1"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	problem := &Problem{
		ID:      "p1",
		Cluster: "debug",
		CPU:     1,
		Memory:  128,
		Workflow: []WorkflowStep{{
			Name:    "judge",
			Image:   "runner",
			Timeout: 60,
			Steps:   [][]string{{"judge"}},
		}},
	}
	submission := &models.Submission{
		ID:        "job-mail-success",
		UserID:    user.ID,
		ProblemID: problem.ID,
		Status:    models.StatusQueued,
		Cluster:   "debug",
		MailType:  "BEGIN,END",
		MailUser:  "ops@example.com",
	}
	if err := db.Create(submission).Error; err != nil {
		t.Fatalf("create submission: %v", err)
	}

	if !scheduler.startJob("debug", QueuedSubmission{Submission: submission, Problem: problem}, scheduler.clusters["debug"].Nodes["n1"], []int{0}) {
		t.Fatalf("start job failed")
	}
	waitForMailEvents(t, mailer, []SlurmMailEvent{SlurmMailBegin, SlurmMailEnd})
}

func TestSchedulerStartsMultiNodeBatchAndReleasesAllNodes(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name: "debug",
			Nodes: []config.Node{
				{Name: "n1", CPU: 4, Memory: 1024},
				{Name: "n2", CPU: 4, Memory: 1024},
			},
		}},
		Storage: config.Storage{
			SubmissionContent: t.TempDir(),
			SubmissionLog:     t.TempDir(),
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	runtime := &fakeBatchRuntime{}
	scheduler.SetRuntimeFactory(func(config.Node) (RuntimeManager, error) {
		return runtime, nil
	})
	user := models.User{ID: "u1", Username: "u1"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	problem := &Problem{
		ID:      "p1",
		Cluster: "debug",
		CPU:     3,
		Memory:  256,
		Workflow: []WorkflowStep{{
			Name:    "judge",
			Image:   "runner",
			Timeout: 60,
			Steps:   [][]string{{"judge"}},
		}},
	}
	submission := &models.Submission{
		ID:        "job-multi-node",
		UserID:    user.ID,
		ProblemID: problem.ID,
		Status:    models.StatusQueued,
		Cluster:   "debug",
		Nodes:     2,
	}
	if err := db.Create(submission).Error; err != nil {
		t.Fatalf("create submission: %v", err)
	}

	allocations := scheduler.findAvailableNodeAllocationsForJob("debug", QueuedSubmission{Submission: submission, Problem: problem}, time.Now())
	if len(allocations) != 2 {
		t.Fatalf("allocations len = %d, want 2", len(allocations))
	}
	if !scheduler.startJobWithAllocations("debug", QueuedSubmission{Submission: submission, Problem: problem}, allocations) {
		t.Fatalf("start multi-node job failed")
	}

	waitForSubmissionStatus(t, db, submission.ID, models.StatusSuccess)
	var updated models.Submission
	if err := db.First(&updated, "id = ?", submission.ID).Error; err != nil {
		t.Fatalf("load updated submission: %v", err)
	}
	if updated.Node != "n1,n2" || updated.AllocatedCores != "0,1" || updated.AllocatedNodeCores != "n1:0,1;n2:0" {
		t.Fatalf("unexpected multi-node allocation fields: %#v", updated)
	}
	if len(runtime.createdEnvs) == 0 {
		t.Fatalf("runtime did not receive container environment")
	}
	env := dispatcherTestEnvMap(runtime.createdEnvs[0])
	if env["SLURM_JOB_NUM_NODES"] != "2" || env["SLURM_NNODES"] != "2" || env["SLURM_CPUS_ON_NODE"] != "2" || env["SLURM_JOB_CPUS_PER_NODE"] != "2,1" || env["SLURM_MEM_PER_NODE"] != "256" {
		t.Fatalf("unexpected multi-node Slurm env: %#v", env)
	}
	for _, nodeName := range []string{"n1", "n2"} {
		node := scheduler.clusters["debug"].Nodes[nodeName]
		if node.UsedMemory != 0 {
			t.Fatalf("node %s memory still allocated: %d", nodeName, node.UsedMemory)
		}
		for coreID, used := range node.UsedCores {
			if used {
				t.Fatalf("node %s core %d still allocated", nodeName, coreID)
			}
		}
	}
}

func dispatcherTestEnvMap(envs []string) map[string]string {
	result := make(map[string]string, len(envs))
	for _, env := range envs {
		name, value, ok := strings.Cut(env, "=")
		if ok {
			result[name] = value
		}
	}
	return result
}

func TestDispatcherSendsFailSlurmMail(t *testing.T) {
	cfg := config.Config{
		Cluster: []config.Cluster{{
			Name:  "debug",
			Nodes: []config.Node{{Name: "n1", CPU: 1, Memory: 128}},
		}},
		Storage: config.Storage{
			SubmissionContent: t.TempDir(),
			SubmissionLog:     t.TempDir(),
		},
	}
	scheduler, db := newPolicyTestScheduler(t, cfg)
	runtime := &fakeBatchRuntime{stdout: "not-json"}
	scheduler.SetRuntimeFactory(func(config.Node) (RuntimeManager, error) {
		return runtime, nil
	})
	mailer := &fakeMailSender{}
	scheduler.SetMailSender(mailer)
	user := models.User{ID: "u1", Username: "u1"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	problem := &Problem{
		ID:      "p1",
		Cluster: "debug",
		CPU:     1,
		Memory:  128,
		Workflow: []WorkflowStep{{
			Name:    "judge",
			Image:   "runner",
			Timeout: 60,
			Steps:   [][]string{{"judge"}},
		}},
	}
	submission := &models.Submission{
		ID:             "job-mail-fail",
		UserID:         user.ID,
		ProblemID:      problem.ID,
		Status:         models.StatusRunning,
		Cluster:        "debug",
		Node:           "n1",
		AllocatedCores: "0",
		MailType:       "FAIL",
		MailUser:       "ops@example.com",
	}
	if err := db.Create(submission).Error; err != nil {
		t.Fatalf("create submission: %v", err)
	}

	scheduler.dispatcher.Dispatch(submission, problem, scheduler.clusters["debug"].Nodes["n1"], []int{0})
	waitForMailEvents(t, mailer, []SlurmMailEvent{SlurmMailFail})
}

func waitForMailEvents(t *testing.T, mailer *fakeMailSender, want []SlurmMailEvent) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got := mailer.Events()
		if len(got) >= len(want) {
			matches := true
			for i := range want {
				if got[i] != want[i] {
					matches = false
					break
				}
			}
			if matches {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("mail events = %#v, want prefix %#v", mailer.Events(), want)
}

func waitForSubmissionStatus(t *testing.T, db *gorm.DB, submissionID string, want models.Status) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var sub models.Submission
		if err := db.First(&sub, "id = ?", submissionID).Error; err != nil {
			t.Fatalf("load submission: %v", err)
		}
		if sub.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("submission %s did not reach status %s", submissionID, want)
}
