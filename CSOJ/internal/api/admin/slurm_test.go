package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"github.com/ZJUSCT/CSOJ/internal/judger"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func TestSlurmSqueueEndpoint(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	now := time.Now()
	if err := db.Create(&models.Submission{
		ID:          "job-1",
		CreatedAt:   now,
		ProblemID:   "p1",
		UserID:      "u1",
		JobName:     "train-run",
		Status:      models.StatusQueued,
		Cluster:     "debug",
		CPU:         2,
		NTasks:      2,
		Nodes:       1,
		CPUsPerTask: 1,
		Memory:      512,
		Licenses:    "foo:1",
		Account:     "course-a",
		QOS:         "normal",
		Hold:        true,
		Reason:      "JobHeld",
		ArrayJobID:  "array-1",
		ArrayTaskID: 7,
	}).Error; err != nil {
		t.Fatalf("create submission: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/squeue?state=PENDING&job_name=train-run&array_job_id=array-1&array_task_id=7&user=u0,u1&partition=other,debug&account=other,course-a&qos=debug,normal&fields=job_id,name,job_name,problem_id,array_job_id,array_task_id,state,reason,partition,cpus,ntasks,cpus_per_task,nodes,memory,licenses", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var items []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &items)
	if len(items) != 1 {
		t.Fatalf("items len = %d, want 1: %#v", len(items), items)
	}
	if items[0]["job_id"] != "job-1" || items[0]["name"] != "train-run" || items[0]["job_name"] != "train-run" || items[0]["problem_id"] != "p1" || items[0]["array_job_id"] != "array-1" || items[0]["array_task_id"].(float64) != 7 || items[0]["state"] != models.SlurmStatePending || items[0]["reason"] != "JobHeld" || items[0]["cpus"].(float64) != 2 || items[0]["ntasks"].(float64) != 2 || items[0]["cpus_per_task"].(float64) != 1 || items[0]["nodes"].(float64) != 1 || items[0]["memory"].(float64) != 512 || items[0]["licenses"] != "foo:1" {
		t.Fatalf("unexpected squeue item: %#v", items[0])
	}
	if _, ok := items[0]["account"]; ok {
		t.Fatalf("fields projection should omit account: %#v", items[0])
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/squeue?states=PD&fields=job_id,state", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("short-state squeue status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &items)
	if len(items) != 1 || items[0]["job_id"] != "job-1" || items[0]["state"] != models.SlurmStatePending {
		t.Fatalf("unexpected short-state squeue items: %#v", items)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/squeue?format=%25i,%25P,%25j,%25u,%25t,%25D,%25R", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("format squeue status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &items)
	if len(items) != 1 || items[0]["job_id"] != "job-1" || items[0]["partition"] != "debug" || items[0]["job_name"] != "train-run" || items[0]["user_id"] != "u1" || items[0]["state"] != models.SlurmStatePending || items[0]["nodes"].(float64) != 1 || items[0]["reason"] != "JobHeld" {
		t.Fatalf("unexpected format squeue items: %#v", items)
	}
	if _, ok := items[0]["account"]; ok {
		t.Fatalf("format projection should omit account: %#v", items[0])
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/squeue?job_id=array-1_7&fields=job_id,array_job_id,array_task_id", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("array job-id selector status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &items)
	if len(items) != 1 || items[0]["job_id"] != "job-1" || items[0]["array_job_id"] != "array-1" || items[0]["array_task_id"].(float64) != 7 {
		t.Fatalf("unexpected array job-id selected squeue item: %#v", items)
	}
}

func TestSlurmScontrolShowJobsEndpoint(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	if err := db.Create(&[]models.Submission{
		{ID: "show-job-pending", ProblemID: "p1", UserID: "u1", Status: models.StatusQueued, Cluster: "debug", Account: "course-a", QOS: "normal", ArrayJobID: "show-array", ArrayTaskID: 2},
		{ID: "show-job-done", ProblemID: "p1", UserID: "u1", Status: models.StatusSuccess, Cluster: "debug", Account: "course-a", QOS: "normal"},
	}).Error; err != nil {
		t.Fatalf("create jobs: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/jobs?state=PENDING&array_job_id=show-array&array_task_id=2&user=u0,u1&partition=other,debug&account=other,course-a&qos=debug,normal&status=Success,Queued&fields=job_id,array_job_id,array_task_id,state,native_status,partition,account", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show jobs status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var jobs []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &jobs)
	if len(jobs) != 1 || jobs[0]["job_id"] != "show-job-pending" || jobs[0]["array_job_id"] != "show-array" || jobs[0]["array_task_id"].(float64) != 2 || jobs[0]["state"] != models.SlurmStatePending || jobs[0]["native_status"] != string(models.StatusQueued) {
		t.Fatalf("unexpected show jobs response: %#v", jobs)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/jobs?states=CD&fields=job_id,state,native_status", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("short-state show jobs status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &jobs)
	if len(jobs) != 1 || jobs[0]["job_id"] != "show-job-done" || jobs[0]["state"] != models.SlurmStateCompleted || jobs[0]["native_status"] != string(models.StatusSuccess) {
		t.Fatalf("unexpected short-state show jobs response: %#v", jobs)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/job/show-array_2?fields=job_id,array_job_id,array_task_id", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show job array selector status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var job map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &job)
	if job["job_id"] != "show-job-pending" || job["array_job_id"] != "show-array" || job["array_task_id"].(float64) != 2 {
		t.Fatalf("unexpected show job array selector response: %#v", job)
	}
}

func TestSlurmSacctEndpoint(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	now := time.Now().UTC().Truncate(time.Second)
	if err := db.Create(&models.AccountingRecord{
		CreatedAt:    now,
		SubmissionID: "job-2",
		UserID:       "u1",
		ProblemID:    "p1",
		JobName:      "named-accounting",
		Cluster:      "debug",
		Account:      "course-a",
		QOS:          "normal",
		ArrayJobID:   "acct-array",
		ArrayTaskID:  4,
		Event:        database.AccountEventInterrupted,
		State:        models.StatusFailed,
		Reason:       "Interrupted",
		BillingUnits: 3,
	}).Error; err != nil {
		t.Fatalf("create accounting record: %v", err)
	}
	if err := db.Create(&models.AccountingRecord{
		CreatedAt:    now.Add(-2 * time.Hour),
		SubmissionID: "job-old",
		UserID:       "u1",
		ProblemID:    "p1",
		Cluster:      "debug",
		Account:      "course-a",
		QOS:          "normal",
		Event:        database.AccountEventSubmitted,
		State:        models.StatusQueued,
	}).Error; err != nil {
		t.Fatalf("create old accounting record: %v", err)
	}
	if err := db.Create(&models.AccountingRecord{
		CreatedAt:    now.Add(-3 * time.Hour),
		SubmissionID: "job-6",
		UserID:       "u1",
		ProblemID:    "p1",
		Cluster:      "debug",
		Account:      "course-a",
		QOS:          "normal",
		ArrayJobID:   "acct-array",
		ArrayTaskID:  6,
		Event:        database.AccountEventSubmitted,
		State:        models.StatusQueued,
	}).Error; err != nil {
		t.Fatalf("create bracket array accounting record: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacct?states=CA&fields=job_id,state,reason,billing_units", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var page struct {
		Items []map[string]interface{} `json:"items"`
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if len(page.Items) != 1 {
		t.Fatalf("items len = %d, want 1: %#v", len(page.Items), page.Items)
	}
	if page.Items[0]["state"] != models.SlurmStateCancelled || page.Items[0]["reason"] != "Cancelled" {
		t.Fatalf("unexpected sacct item: %#v", page.Items[0])
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacct?user=u0,u1&partition=other,debug&account=other,course-a&qos=debug,normal&native_state=Queued,Failed&event=Started,Interrupted&fields=job_id", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("list-filtered sacct status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if len(page.Items) != 1 || page.Items[0]["job_id"] != "job-2" {
		t.Fatalf("unexpected list-filtered sacct items: %#v", page.Items)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacct?job_name=named-accounting&fields=job_id,job_name,problem_id", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("job-name status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if len(page.Items) != 1 || page.Items[0]["job_id"] != "job-2" || page.Items[0]["job_name"] != "named-accounting" || page.Items[0]["problem_id"] != "p1" {
		t.Fatalf("unexpected job-name sacct items: %#v", page.Items)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacct?array_job_id=acct-array&array_task_id=4&fields=job_id,array_job_id,array_task_id", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("array-filtered status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if len(page.Items) != 1 || page.Items[0]["job_id"] != "job-2" || page.Items[0]["array_job_id"] != "acct-array" || page.Items[0]["array_task_id"].(float64) != 4 {
		t.Fatalf("unexpected array-filtered sacct items: %#v", page.Items)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacct?job_id=acct-array_4&fields=job_id,array_job_id,array_task_id", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("array job-id sacct status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if len(page.Items) != 1 || page.Items[0]["job_id"] != "job-2" || page.Items[0]["array_job_id"] != "acct-array" || page.Items[0]["array_task_id"].(float64) != 4 {
		t.Fatalf("unexpected array job-id sacct items: %#v", page.Items)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacct?job_id=acct-array_%5B4,6%5D&fields=job_id,array_job_id,array_task_id", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("bracket array job-id sacct status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	seenArrayTasks := make(map[float64]bool)
	for _, item := range page.Items {
		if item["array_job_id"] != "acct-array" {
			t.Fatalf("unexpected bracket array job-id item: %#v", item)
		}
		seenArrayTasks[item["array_task_id"].(float64)] = true
	}
	if len(page.Items) != 2 || !seenArrayTasks[4] || !seenArrayTasks[6] {
		t.Fatalf("unexpected bracket array job-id sacct items: %#v", page.Items)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacct?job_id=job-2,job-old&fields=job_id", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("multi-job status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	seenJobs := make(map[string]bool)
	for _, item := range page.Items {
		seenJobs[item["job_id"].(string)] = true
	}
	if len(page.Items) != 2 || !seenJobs["job-2"] || !seenJobs["job-old"] {
		t.Fatalf("unexpected multi-job sacct items: %#v", page.Items)
	}

	startTime := now.Add(-30 * time.Minute).Format(time.RFC3339)
	endTime := now.Add(30 * time.Minute).Format(time.RFC3339)
	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacct?starttime="+startTime+"&endtime="+endTime+"&fields=job_id", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("time-filtered status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if len(page.Items) != 1 || page.Items[0]["job_id"] != "job-2" {
		t.Fatalf("unexpected time-filtered sacct items: %#v", page.Items)
	}
}

func TestSlurmSbatchCreatesQueuedArraySubmissions(t *testing.T) {
	router, db, scheduler := newSlurmTestRouterWithScheduler(t)
	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/sbatch", `{"user":"u1","problem_id":"p1","cluster":"debug","account":"course-a","qos":"normal","hold":true,"memory":"2G","begin":"2030-01-02 03:04:05","deadline":"2030-01-03T04:05:06","time":"2-03:04","nodelist":"n[01-02]","exclude_nodes":"n03","array":"1-2","files":{"main.txt":"hello"}}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("sbatch status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &response)
	if response["job_id"] == "" || response["state"] != models.SlurmStatePending || response["array_job_id"] == "" || response["requested_nodelist"] != "n[01-02]" || response["exclude_nodes"] != "n03" {
		t.Fatalf("unexpected sbatch response: %#v", response)
	}

	var submissions []models.Submission
	if err := db.Where("problem_id = ?", "p1").Order("array_task_id asc").Find(&submissions).Error; err != nil {
		t.Fatalf("load submissions: %v", err)
	}
	if len(submissions) != 2 {
		t.Fatalf("submissions len = %d, want 2", len(submissions))
	}
	for _, sub := range submissions {
		if sub.Status != models.StatusQueued || sub.Cluster != "debug" || sub.Account != "course-a" || sub.QOS != "normal" || sub.Memory != 2048 || !sub.Hold || sub.Reason != "JobHeld" || sub.TimeLimit != 2*24*3600+3*3600+4*60 {
			t.Fatalf("unexpected sbatch submission: %#v", sub)
		}
		if sub.NodeList != "n[01-02]" || sub.ExcludeNodes != "n03" {
			t.Fatalf("unexpected sbatch node filters: %#v", sub)
		}
		if sub.BeginTime == nil || sub.BeginTime.Format("2006-01-02 15:04:05") != "2030-01-02 03:04:05" || sub.Deadline == nil || sub.Deadline.Format("2006-01-02 15:04:05") != "2030-01-03 04:05:06" {
			t.Fatalf("unexpected sbatch timing: begin=%v deadline=%v", sub.BeginTime, sub.Deadline)
		}
		if sub.ArrayJobID == "" || sub.ArrayTaskCount != 2 {
			t.Fatalf("array metadata missing: %#v", sub)
		}
	}
	if lengths := scheduler.GetQueueLengths(); lengths["debug"] != 2 {
		t.Fatalf("queue lengths = %#v, want debug=2", lengths)
	}

	var events int64
	if err := db.Model(&models.AccountingRecord{}).Where("problem_id = ? AND event = ?", "p1", database.AccountEventSubmitted).Count(&events).Error; err != nil {
		t.Fatalf("count submitted accounting: %v", err)
	}
	if events != 2 {
		t.Fatalf("submitted events = %d, want 2", events)
	}
}

func TestSlurmBatchWrapWritesScript(t *testing.T) {
	req := slurmBatchRequest{Wrap: "echo wrapped"}
	applySlurmBatchWrap(&req)

	dir := t.TempDir()
	if err := writeSlurmBatchFiles(dir, req); err != nil {
		t.Fatalf("write wrapped script: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "sbatch.sh"))
	if err != nil {
		t.Fatalf("read wrapped script: %v", err)
	}
	if string(data) != "#!/bin/sh\necho wrapped\n" {
		t.Fatalf("unexpected wrapped script: %q", string(data))
	}
}

func TestSlurmSbatchAcceptsMultiNodeJobs(t *testing.T) {
	router, db, _ := newSlurmTestRouterWithScheduler(t)
	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/sbatch", `{"user_id":"u1","problem_id":"p1","nodes":2}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("multi-node sbatch status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var sub models.Submission
	if err := db.Order("created_at desc").First(&sub, "problem_id = ?", "p1").Error; err != nil {
		t.Fatalf("load multi-node submission: %v", err)
	}
	if sub.Nodes != 2 {
		t.Fatalf("multi-node submission nodes = %d, want 2", sub.Nodes)
	}

	script := "#!/bin/sh\n#SBATCH -N 2-4\necho run\n"
	bodyData, err := json.Marshal(map[string]interface{}{
		"user_id":    "u1",
		"problem_id": "p1",
		"script":     script,
	})
	if err != nil {
		t.Fatalf("marshal script body: %v", err)
	}
	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/sbatch", string(bodyData))
	if recorder.Code != http.StatusOK {
		t.Fatalf("multi-node range sbatch status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if err := db.Order("created_at desc").First(&sub, "problem_id = ?", "p1").Error; err != nil {
		t.Fatalf("load multi-node range submission: %v", err)
	}
	if sub.Nodes != 2 {
		t.Fatalf("multi-node range submission nodes = %d, want 2", sub.Nodes)
	}
}

func TestParseSlurmMemoryMB(t *testing.T) {
	cases := map[string]int64{
		"512":     512,
		"512M":    512,
		"512MB":   512,
		"512MiB":  512,
		"2G":      2048,
		"2GB":     2048,
		"2GiB":    2048,
		"1T":      1024 * 1024,
		"1TB":     1024 * 1024,
		"1025K":   2,
		"1025KB":  2,
		"1025KiB": 2,
	}
	for input, expected := range cases {
		actual, err := parseSlurmMemoryMB(input)
		if err != nil {
			t.Fatalf("parseSlurmMemoryMB(%q): %v", input, err)
		}
		if actual != expected {
			t.Fatalf("parseSlurmMemoryMB(%q) = %d, want %d", input, actual, expected)
		}
	}

	if _, err := parseSlurmMemoryMB("1PB"); err == nil {
		t.Fatalf("expected unsupported unit to fail")
	}
}

func TestParseSlurmTimeLimit(t *testing.T) {
	cases := map[string]int{
		"90":         90 * 60,
		"04:05":      4*60 + 5,
		"1:02:03":    1*3600 + 2*60 + 3,
		"0-03":       3 * 3600,
		"0-00:30":    30 * 60,
		"2-03":       2*24*3600 + 3*3600,
		"2-03:04":    2*24*3600 + 3*3600 + 4*60,
		"2-03:04:05": 2*24*3600 + 3*3600 + 4*60 + 5,
	}
	for input, expected := range cases {
		actual, err := parseSlurmTimeLimit(input)
		if err != nil {
			t.Fatalf("parseSlurmTimeLimit(%q): %v", input, err)
		}
		if actual != expected {
			t.Fatalf("parseSlurmTimeLimit(%q) = %d, want %d", input, actual, expected)
		}
	}

	for _, input := range []string{"", "0", "1:99", "1:02:99", "2-03:99", "2-03:04:99", "1-"} {
		if _, err := parseSlurmTimeLimit(input); err == nil {
			t.Fatalf("parseSlurmTimeLimit(%q) should fail", input)
		}
	}
}

func TestSlurmStateFilterMatchesAliases(t *testing.T) {
	cases := []struct {
		filter string
		states []string
		want   bool
	}{
		{filter: "PD", states: []string{models.SlurmStatePending}, want: true},
		{filter: "R", states: []string{models.SlurmStateRunning}, want: true},
		{filter: "S", states: []string{models.SlurmStateSuspended}, want: true},
		{filter: "CD", states: []string{string(models.StatusSuccess)}, want: true},
		{filter: "CA", states: []string{models.SlurmStateCancelled}, want: true},
		{filter: "TO", states: []string{models.SlurmStateTimeout}, want: true},
		{filter: "NF", states: []string{models.SlurmStateNodeFail}, want: true},
		{filter: "OOM", states: []string{models.SlurmStateOOM}, want: true},
		{filter: "PR", states: []string{models.SlurmStatePreempted}, want: true},
		{filter: "mix", states: []string{"MIXED"}, want: true},
		{filter: "PD,R", states: []string{models.SlurmStateRunning}, want: true},
		{filter: "PD", states: []string{models.SlurmStateRunning}, want: false},
	}

	for _, tc := range cases {
		if got := slurmStateFilterMatches(tc.filter, tc.states...); got != tc.want {
			t.Fatalf("slurmStateFilterMatches(%q, %#v) = %v, want %v", tc.filter, tc.states, got, tc.want)
		}
	}
}

func TestSlurmFieldListAcceptsFormatAliases(t *testing.T) {
	cases := map[string]string{
		"%.18i %.9P %.8j %.8u %.2t %.6D %R":       "job_id,partition,job_name,user_id,state,nodes,reason",
		"JobID,JobName,AllocCPUS,ExitCode,MaxRSS": "job_id,job_name,alloc_cpus,exit_code,max_rss",
		"job_id,name,allowed_qos":                 "job_id,name,allowed_qos",
	}
	for input, expected := range cases {
		if actual := strings.Join(slurmFieldList(input), ","); actual != expected {
			t.Fatalf("slurmFieldList(%q) = %q, want %q", input, actual, expected)
		}
	}

	if fields := slurmFieldList("%all"); fields != nil {
		t.Fatalf("slurmFieldList(\"%%all\") = %#v, want nil", fields)
	}
}

func TestSlurmSbatchParsesScriptDirectives(t *testing.T) {
	router, db, _ := newSlurmTestRouterWithScheduler(t)
	script := "#!/bin/sh\n" +
		"#SBATCH -pignored\n" +
		"#SBATCH -Jtrain-model\n" +
		"#SBATCH -D/scratch/u1\n" +
		"#SBATCH -iinput.txt\n" +
		"#SBATCH -ologs/%j.out\n" +
		"#SBATCH -elogs/%j.err\n" +
		"#SBATCH --open-mode=append\n" +
		"#SBATCH --comment \"night run\"\n" +
		"#SBATCH --mail-type=END,FAIL\n" +
		"#SBATCH --mail-user=user@example.com\n" +
		"#SBATCH --exclusive\n" +
		"#SBATCH --requeue\n" +
		"#SBATCH --export=ALL,FOO=bar,EMPTY\n" +
		"#SBATCH -Acourse-a\n" +
		"#SBATCH --qos normal\n" +
		"#SBATCH --priority=17\n" +
		"#SBATCH --nice 3\n" +
		"#SBATCH -n3\n" +
		"#SBATCH -c2\n" +
		"#SBATCH -N1-2\n" +
		"#SBATCH --mem-per-cpu=256MiB\n" +
		"#SBATCH --hold\n" +
		"#SBATCH -t1:02:03\n" +
		"#SBATCH -dafterok:dep-a:dep-b\n" +
		"#SBATCH --reservation=res-a\n" +
		"#SBATCH -wn[02-03]\n" +
		"#SBATCH -xn03\n" +
		"#SBATCH -Cgpu&avx\n" +
		"#SBATCH --gres=gpu:1\n" +
		"#SBATCH --tres=license/foo:1\n" +
		"#SBATCH --licenses=bar:2\n" +
		"#SBATCH -a3-4%1\n" +
		"echo run\n"
	bodyData, err := json.Marshal(map[string]interface{}{
		"user_id":    "u1",
		"problem_id": "p1",
		"partition":  "debug",
		"environment": map[string]string{
			"FOO": "json",
			"BAR": "baz",
		},
		"script": script,
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/sbatch", string(bodyData))
	if recorder.Code != http.StatusOK {
		t.Fatalf("sbatch status = %d body=%s", recorder.Code, recorder.Body.String())
	}

	var submissions []models.Submission
	if err := db.Where("problem_id = ?", "p1").Order("array_task_id asc").Find(&submissions).Error; err != nil {
		t.Fatalf("load submissions: %v", err)
	}
	if len(submissions) != 2 {
		t.Fatalf("submissions len = %d, want 2", len(submissions))
	}
	for _, sub := range submissions {
		if sub.Cluster != "debug" || sub.Account != "course-a" || sub.QOS != "normal" || sub.Priority != 17 || sub.Nice != 3 || sub.CPU != 6 || sub.NTasks != 3 || sub.CPUsPerTask != 2 || sub.Nodes != 1 || sub.Memory != 1536 || !sub.Hold || sub.Reason != "JobHeld" {
			t.Fatalf("unexpected directive scheduling fields: %#v", sub)
		}
		if sub.TimeLimit != 3723 || sub.Dependencies != "afterok:dep-a:dep-b" || sub.Reservation != "res-a" || sub.NodeList != "n[02-03]" || sub.ExcludeNodes != "n03" || sub.Constraint != "gpu&avx" || sub.GRES != "gpu:1" || sub.TRES != "license/foo:1,license/bar:2" || sub.Licenses != "bar:2" {
			t.Fatalf("unexpected directive resources: %#v", sub)
		}
		if sub.ArraySpec != "3-4%1" || sub.ArrayTaskCount != 2 || sub.ArrayMaxRunning != 1 {
			t.Fatalf("unexpected directive array metadata: %#v", sub)
		}
		if sub.JobName != "train-model" || sub.WorkDir != "/scratch/u1" || sub.StdinPath != "input.txt" || sub.StdoutPath != "logs/%j.out" || sub.StderrPath != "logs/%j.err" || sub.OpenMode != "append" || sub.Comment != "night run" {
			t.Fatalf("unexpected directive job metadata: %#v", sub)
		}
		if sub.MailType != "END,FAIL" || sub.MailUser != "user@example.com" || !sub.Exclusive || !sub.Requeue {
			t.Fatalf("unexpected directive policy metadata: %#v", sub)
		}
		if sub.ExportEnv != "ALL,FOO=bar,EMPTY" || sub.Environment["FOO"] != "json" || sub.Environment["BAR"] != "baz" || sub.Environment["EMPTY"] != "" {
			t.Fatalf("unexpected directive environment: export=%q env=%#v", sub.ExportEnv, sub.Environment)
		}
	}
}

func TestSlurmSbatchAppliesDefaultOutputPath(t *testing.T) {
	router, db, _ := newSlurmTestRouterWithScheduler(t)
	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/sbatch", `{"user_id":"u1","problem_id":"p1","wrap":"echo ok"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("sbatch status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var sub models.Submission
	if err := db.Where("problem_id = ?", "p1").First(&sub).Error; err != nil {
		t.Fatalf("load submission: %v", err)
	}
	if sub.StdoutPath != "slurm-%j.out" || sub.StderrPath != "" {
		t.Fatalf("unexpected default output paths: stdout=%q stderr=%q", sub.StdoutPath, sub.StderrPath)
	}
}

func TestSlurmScontrolUpdateJobAndNode(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	if err := db.Create(&models.Submission{
		ID:        "job-3",
		ProblemID: "p1",
		UserID:    "u1",
		Status:    models.StatusQueued,
		Cluster:   "debug",
	}).Error; err != nil {
		t.Fatalf("create submission: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scontrol/update/job/job-3", `{"hold":true,"priority":100,"ntasks":2,"cpus_per_task":3,"nodes":1,"mem":"2G","start_time":"2031-02-03 04:05:06","deadline":"2031-02-04T05:06:07","time":"2-03:04","job_name":"updated-name","chdir":"/tmp/run","input":"in.txt","output":"out.txt","error":"err.txt","open_mode":"truncate","comment":"manual update","mail_type":"ALL","mail_user":"ops@example.com","exclusive":true,"requeue":true,"nodelist":"n2","exclude":"n3","constraint":"gpu","gres":"gpu:1","tres":"gres/gpu:1","licenses":"foo:2","export":"ALL,UPD=1","environment":{"UPD":"2","NEW":"x"}}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("job update status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var sub models.Submission
	if err := db.Where("id = ?", "job-3").First(&sub).Error; err != nil {
		t.Fatalf("load submission: %v", err)
	}
	if !sub.Hold || sub.Reason != "JobHeld" || sub.Priority != 100 || sub.CPU != 6 || sub.NTasks != 2 || sub.CPUsPerTask != 3 || sub.Nodes != 1 || sub.Memory != 2048 || sub.TimeLimit != 2*24*3600+3*3600+4*60 {
		t.Fatalf("unexpected updated submission: %#v", sub)
	}
	if sub.BeginTime == nil || sub.BeginTime.Format("2006-01-02 15:04:05") != "2031-02-03 04:05:06" || sub.Deadline == nil || sub.Deadline.Format("2006-01-02 15:04:05") != "2031-02-04 05:06:07" {
		t.Fatalf("unexpected updated timing: begin=%v deadline=%v", sub.BeginTime, sub.Deadline)
	}
	if sub.JobName != "updated-name" || sub.WorkDir != "/tmp/run" || sub.StdinPath != "in.txt" || sub.StdoutPath != "out.txt" || sub.StderrPath != "err.txt" || sub.OpenMode != "truncate" || sub.Comment != "manual update" {
		t.Fatalf("unexpected updated job metadata: %#v", sub)
	}
	if sub.MailType != "ALL" || sub.MailUser != "ops@example.com" || !sub.Exclusive || !sub.Requeue {
		t.Fatalf("unexpected updated policy metadata: %#v", sub)
	}
	if sub.NodeList != "n2" || sub.ExcludeNodes != "n3" || sub.Constraint != "gpu" || sub.GRES != "gpu:1" || sub.Licenses != "foo:2" || sub.TRES != "gres/gpu:1,license/foo:2" {
		t.Fatalf("unexpected updated resources: %#v", sub)
	}
	if sub.ExportEnv != "ALL,UPD=1" || sub.Environment["UPD"] != "2" || sub.Environment["NEW"] != "x" {
		t.Fatalf("unexpected updated environment: export=%q env=%#v", sub.ExportEnv, sub.Environment)
	}
	var heldEvents int64
	if err := db.Model(&models.AccountingRecord{}).Where("submission_id = ? AND event = ?", "job-3", database.AccountEventHeld).Count(&heldEvents).Error; err != nil {
		t.Fatalf("count accounting: %v", err)
	}
	if heldEvents != 1 {
		t.Fatalf("held events = %d, want 1", heldEvents)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scontrol/update/node/debug/n1", `{"state":"down","reason":"maintenance"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("node update status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var node map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &node)
	if node["state"] != "DOWN" || node["native_state"] != "down" || node["reason"] != "maintenance" {
		t.Fatalf("unexpected node response: %#v", node)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/partition?fields=partition,state,priority_tier,node_count,total_cpus", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show partition status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var partitions []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &partitions)
	if len(partitions) != 1 || partitions[0]["partition"] != "debug" || partitions[0]["state"] != "UP" || partitions[0]["priority_tier"].(float64) != 2 || partitions[0]["node_count"].(float64) != 1 {
		t.Fatalf("unexpected partitions: %#v", partitions)
	}

	recorder = performSlurmRequest(router, http.MethodPatch, "/api/v1/slurm/scontrol/update/partition/debug", `{"state":"DOWN","priority":5,"max_time":"01:00:00","max_jobs":2,"allow_users":"alice,bob","allow_accounts":"course-a course-b","allow_qos":"normal","deny_qos":"debug"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("partition update status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var partition map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &partition)
	if partition["partition"] != "debug" || partition["state"] != "DOWN" || partition["priority_tier"].(float64) != 5 || partition["max_time"].(float64) != 3600 || partition["max_jobs"].(float64) != 2 {
		t.Fatalf("unexpected updated partition: %#v", partition)
	}
	allowUsers := partition["allow_users"].([]interface{})
	allowAccounts := partition["allow_accounts"].([]interface{})
	allowQOS := partition["allow_qos"].([]interface{})
	denyQOS := partition["deny_qos"].([]interface{})
	if len(allowUsers) != 2 || allowUsers[1] != "bob" || len(allowAccounts) != 2 || allowAccounts[1] != "course-b" || len(allowQOS) != 1 || allowQOS[0] != "normal" || len(denyQOS) != 1 || denyQOS[0] != "debug" {
		t.Fatalf("partition list aliases were not parsed: %#v", partition)
	}
}

func TestSlurmShowLicensesEndpoint(t *testing.T) {
	router, _ := newSlurmTestRouter(t)
	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/licenses?fields=license,total,available", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var items []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &items)
	if len(items) != 1 {
		t.Fatalf("items len = %d, want 1: %#v", len(items), items)
	}
	if items[0]["license"] != "license/foo" || items[0]["total"].(float64) != 2 || items[0]["available"].(float64) != 2 {
		t.Fatalf("unexpected license item: %#v", items[0])
	}
	if _, ok := items[0]["used"]; ok {
		t.Fatalf("fields projection should omit used: %#v", items[0])
	}
}

func TestSlurmSinfoEndpoint(t *testing.T) {
	router, _ := newSlurmTestRouter(t)
	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sinfo?fields=partition,nodelist,state,cpus,runtime", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var items []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &items)
	if len(items) != 1 {
		t.Fatalf("items len = %d, want 1: %#v", len(items), items)
	}
	if items[0]["partition"] != "debug" || items[0]["nodelist"] != "n1" || items[0]["state"] != "IDLE" || items[0]["cpus"].(float64) != 4 || items[0]["runtime"] != judger.RuntimeDocker {
		t.Fatalf("unexpected sinfo item: %#v", items[0])
	}
	if _, ok := items[0]["memory"]; ok {
		t.Fatalf("fields projection should omit memory: %#v", items[0])
	}
}

func TestSlurmScontrolShowHostnamesAndHostlist(t *testing.T) {
	router, _ := newSlurmTestRouterWithNodes(t, []config.Node{
		{Name: "n01", CPU: 4, Memory: 1024},
		{Name: "n02", CPU: 4, Memory: 1024},
		{Name: "n03", CPU: 4, Memory: 1024},
		{Name: "gpu-1", CPU: 8, Memory: 4096},
		{Name: "gpu-3", CPU: 8, Memory: 4096},
	})

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/hostnames?hostlist=n%5B01-03%5D,gpu-%5B1-3:2%5D&fields=hostlist,hostnames,count", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show hostnames status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &response)
	hostnames := response["hostnames"].([]interface{})
	if response["hostlist"] != "n[01-03],gpu-[1-3:2]" || response["count"].(float64) != 5 || len(hostnames) != 5 || hostnames[0] != "n01" || hostnames[2] != "n03" || hostnames[3] != "gpu-1" || hostnames[4] != "gpu-3" {
		t.Fatalf("unexpected hostnames response: %#v", response)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scontrol/show/hostlist?fields=hostlist,hostnames,count", `{"hostnames":["gpu-3","gpu-1","n03","n01","n02","n02"]}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("show hostlist status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &response)
	if response["hostlist"] != "gpu-[1,3],n[01-03]" || response["count"].(float64) != 5 {
		t.Fatalf("unexpected hostlist response: %#v", response)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/hostnames?fields=hostnames,count", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show configured hostnames status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &response)
	if response["count"].(float64) != 5 {
		t.Fatalf("unexpected configured hostnames response: %#v", response)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/hostnames?hostlist=n%5B01-03", "")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid hostnames status = %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestSlurmScontrolShowStepsEndpoint(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	startedAt := time.Now().Add(-2 * time.Minute)
	finishedAt := time.Now().Add(-time.Minute)
	if err := db.Create(&models.RunStep{
		ID:             "step-control",
		AllocationID:   "alloc-control",
		UserID:         "u1",
		Cluster:        "debug",
		Node:           "n1",
		ContainerID:    "container-control",
		Command:        "echo step",
		Status:         models.StatusSuccess,
		ExitCode:       0,
		Stdout:         "step\n",
		CPU:            2,
		Memory:         256,
		AllocatedCores: "0,1",
		StartedAt:      startedAt,
		FinishedAt:     finishedAt,
	}).Error; err != nil {
		t.Fatalf("create run step: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/steps?job_id=alloc-control&job_step_id=alloc-control.step-control&states=CD&fields=step_id,job_step_id,job_id,state,partition,node,cpus,memory,allocated_cores,exit_code,stdout", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show steps status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var steps []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &steps)
	if len(steps) != 1 || steps[0]["step_id"] != "step-control" || steps[0]["job_step_id"] != "alloc-control.step-control" || steps[0]["job_id"] != "alloc-control" || steps[0]["state"] != models.SlurmStateCompleted || steps[0]["partition"] != "debug" || steps[0]["node"] != "n1" || steps[0]["cpus"].(float64) != 2 || steps[0]["memory"].(float64) != 256 || steps[0]["allocated_cores"] != "0,1" || steps[0]["exit_code"].(float64) != 0 || steps[0]["stdout"] != "step\n" {
		t.Fatalf("unexpected show steps response: %#v", steps)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/step/alloc-control.step-control?fields=step_id,job_step_id,state,stdout", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show step status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var step map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &step)
	if step["step_id"] != "step-control" || step["job_step_id"] != "alloc-control.step-control" || step["state"] != models.SlurmStateCompleted || step["stdout"] != "step\n" {
		t.Fatalf("unexpected show step response: %#v", step)
	}
}

func TestSlurmNodeStateReflectsAllocatedResources(t *testing.T) {
	router, _ := newSlurmTestRouter(t)
	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user_id":"u1","partition":"debug","cpus":2,"memory":512}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("first salloc status = %d body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sinfo?fields=partition,node,state,native_state,alloc_cpus,idle_cpus,alloc_memory,idle_memory", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("sinfo status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var infos []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &infos)
	if len(infos) != 1 || infos[0]["state"] != "MIXED" || infos[0]["native_state"] != "mixed" || infos[0]["alloc_cpus"].(float64) != 2 || infos[0]["idle_cpus"].(float64) != 2 || infos[0]["alloc_memory"].(float64) != 512 {
		t.Fatalf("unexpected mixed sinfo state: %#v", infos)
	}
	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/nodes?fields=node,state,native_state,alloc_cpus,idle_cpus", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show nodes status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var nodes []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &nodes)
	if len(nodes) != 1 || nodes[0]["state"] != "MIXED" || nodes[0]["native_state"] != "mixed" || nodes[0]["alloc_cpus"].(float64) != 2 || nodes[0]["idle_cpus"].(float64) != 2 {
		t.Fatalf("unexpected show nodes response: %#v", nodes)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user_id":"u1","partition":"debug","cpus":2,"memory":512}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("second salloc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/node/debug/n1?fields=node,state,native_state,alloc_cpus,idle_cpus,alloc_memory,idle_memory", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show node status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var node map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &node)
	if node["state"] != "ALLOCATED" || node["native_state"] != "allocated" || node["alloc_cpus"].(float64) != 4 || node["idle_cpus"].(float64) != 0 || node["alloc_memory"].(float64) != 1024 {
		t.Fatalf("unexpected allocated node state: %#v", node)
	}
}

func TestSlurmSprioEndpoint(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	now := time.Now().Add(-10 * time.Minute)
	if err := db.Create(&models.Submission{
		ID:        "job-prio",
		CreatedAt: now,
		ProblemID: "p1",
		UserID:    "u1",
		Status:    models.StatusQueued,
		Cluster:   "debug",
		Account:   "course-a",
		QOS:       "normal",
		Priority:  7,
		Nice:      3,
	}).Error; err != nil {
		t.Fatalf("create submission: %v", err)
	}
	if err := db.Create(&[]models.Submission{
		{ID: "job-prio-array-2", CreatedAt: now, ProblemID: "p1", UserID: "u1", Status: models.StatusQueued, Cluster: "debug", Account: "course-a", QOS: "normal", Priority: 5, ArrayJobID: "prio-array", ArrayTaskID: 2},
		{ID: "job-prio-array-3", CreatedAt: now, ProblemID: "p1", UserID: "u1", Status: models.StatusQueued, Cluster: "debug", Account: "course-a", QOS: "normal", Priority: 5, ArrayJobID: "prio-array", ArrayTaskID: 3},
	}).Error; err != nil {
		t.Fatalf("create array priority submissions: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sprio?job_id=job-prio&user=u0,u1&partition=other,debug&account=other,course-a&qos=debug,normal&state=PD&fields=job_id,priority,partition_priority,qos_priority,fairshare_priority,job_size_priority,nice_penalty,state", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var items []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &items)
	if len(items) != 1 {
		t.Fatalf("items len = %d, want 1: %#v", len(items), items)
	}
	item := items[0]
	if item["job_id"] != "job-prio" || item["state"] != models.SlurmStatePending {
		t.Fatalf("unexpected sprio item: %#v", item)
	}
	if item["partition_priority"].(float64) != 200 || item["qos_priority"].(float64) != 40 || item["fairshare_priority"].(float64) != 100 || item["job_size_priority"].(float64) != 3 || item["nice_penalty"].(float64) != 6 {
		t.Fatalf("unexpected priority components: %#v", item)
	}
	if item["priority"].(float64) <= 0 {
		t.Fatalf("priority should be positive: %#v", item)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sprio?job_id=prio-array_%5B2,3%5D&fields=job_id,array_job_id,array_task_id,state", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("array sprio status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &items)
	seenTasks := make(map[float64]bool)
	for _, item := range items {
		if item["array_job_id"] != "prio-array" || item["state"] != models.SlurmStatePending {
			t.Fatalf("unexpected array sprio item: %#v", item)
		}
		seenTasks[item["array_task_id"].(float64)] = true
	}
	if len(items) != 2 || !seenTasks[2] || !seenTasks[3] {
		t.Fatalf("unexpected array sprio items: %#v", items)
	}
}

func TestSlurmSshareEndpoint(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	if err := db.Create(&models.AccountingRecord{
		CreatedAt:    time.Now(),
		Account:      "course-a",
		Event:        database.AccountEventCompleted,
		BillingUnits: 20,
	}).Error; err != nil {
		t.Fatalf("create accounting record: %v", err)
	}
	if err := db.Create(&models.Submission{
		ID:        "job-share",
		ProblemID: "p1",
		UserID:    "u1",
		Status:    models.StatusRunning,
		Cluster:   "debug",
		Account:   "course-a",
	}).Error; err != nil {
		t.Fatalf("create running submission: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sshare?account=course-a&fields=account,raw_shares,raw_usage,effective_usage,usage_penalty,running_jobs,submitted_jobs", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var items []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &items)
	if len(items) != 1 {
		t.Fatalf("items len = %d, want 1: %#v", len(items), items)
	}
	item := items[0]
	if item["account"] != "course-a" || item["raw_shares"].(float64) != 20 {
		t.Fatalf("unexpected sshare item: %#v", item)
	}
	if item["raw_usage"].(float64) <= 0 || item["effective_usage"].(float64) <= 0 || item["usage_penalty"].(float64) <= 0 {
		t.Fatalf("usage fields should be positive: %#v", item)
	}
	if item["running_jobs"].(float64) != 1 || item["submitted_jobs"].(float64) != 1 {
		t.Fatalf("unexpected job counts: %#v", item)
	}
}

func TestSlurmSreportEndpoint(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	records := []models.AccountingRecord{
		{CreatedAt: time.Now().Add(-3 * time.Minute), SubmissionID: "job-a", UserID: "u1", Account: "course-a", Cluster: "debug", Event: database.AccountEventCompleted, State: models.StatusSuccess, CPU: 2, Memory: 512, BillingUnits: 3},
		{CreatedAt: time.Now().Add(-2 * time.Minute), SubmissionID: "job-b", UserID: "u2", Account: "course-a", Cluster: "debug", Event: database.AccountEventFailed, State: models.StatusFailed, CPU: 1, Memory: 256, BillingUnits: 2},
		{CreatedAt: time.Now().Add(-1 * time.Minute), SubmissionID: "job-c", UserID: "u1", Account: "course-b", Cluster: "debug", Event: database.AccountEventSubmitted, State: models.StatusQueued, CPU: 8, Memory: 4096, BillingUnits: 99},
	}
	for i := range records {
		if err := db.Create(&records[i]).Error; err != nil {
			t.Fatalf("create accounting record: %v", err)
		}
	}

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sreport?account=course-a&fields=account,jobs,records,alloc_cpus,alloc_mem,billing_units", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("sreport status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var reports []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &reports)
	if len(reports) != 1 {
		t.Fatalf("reports len = %d, want 1: %#v", len(reports), reports)
	}
	if reports[0]["account"] != "course-a" || reports[0]["jobs"].(float64) != 2 || reports[0]["records"].(float64) != 2 || reports[0]["alloc_cpus"].(float64) != 3 || reports[0]["alloc_mem"].(float64) != 768 || reports[0]["billing_units"].(float64) != 5 {
		t.Fatalf("unexpected account report: %#v", reports[0])
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sreport?group_by=user&account=course-a&state=F&fields=user_id,jobs,billing_units", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("sreport grouped status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &reports)
	if len(reports) != 1 || reports[0]["user_id"] != "u2" || reports[0]["jobs"].(float64) != 1 || reports[0]["billing_units"].(float64) != 2 {
		t.Fatalf("unexpected user report: %#v", reports)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sreport?event=Submitted&fields=account,jobs,billing_units", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("sreport explicit event status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &reports)
	if len(reports) != 1 || reports[0]["account"] != "course-b" || reports[0]["jobs"].(float64) != 1 || reports[0]["billing_units"].(float64) != 99 {
		t.Fatalf("explicit event report should include submitted records: %#v", reports)
	}
}

func TestSlurmSeffEndpoint(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	now := time.Now().UTC().Truncate(time.Second)
	records := []models.AccountingRecord{
		{CreatedAt: now.Add(-6 * time.Minute), SubmissionID: "job-eff", UserID: "u1", ProblemID: "p1", JobName: "efficient", Cluster: "debug", Account: "course-a", QOS: "normal", Event: database.AccountEventSubmitted, State: models.StatusQueued},
		{CreatedAt: now.Add(-5 * time.Minute), SubmissionID: "job-eff", UserID: "u1", ProblemID: "p1", JobName: "efficient", Cluster: "debug", Node: "n1", Account: "course-a", QOS: "normal", Event: database.AccountEventStarted, State: models.StatusRunning, CPU: 2, Memory: 1024, BillingUnits: 3},
		{CreatedAt: now, SubmissionID: "job-eff", UserID: "u1", ProblemID: "p1", JobName: "efficient", Cluster: "debug", Node: "n1", Account: "course-a", QOS: "normal", Event: database.AccountEventCompleted, State: models.StatusSuccess, CPU: 2, Memory: 1024, BillingUnits: 3},
	}
	for i := range records {
		if err := db.Create(&records[i]).Error; err != nil {
			t.Fatalf("create seff accounting record: %v", err)
		}
	}
	if err := db.Create(&models.RunStep{
		ID:           "eff-step",
		AllocationID: "job-eff",
		UserID:       "u1",
		Cluster:      "debug",
		Node:         "n1",
		Status:       models.StatusSuccess,
		CPU:          2,
		Memory:       1024,
		AveCPU:       240,
		MaxRSS:       512 * 1024 * 1024,
	}).Error; err != nil {
		t.Fatalf("create seff run step: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/seff/job-eff?fields=job_id,job_name,state,elapsed_seconds,alloc_cpus,alloc_mem,allocated_cpu_seconds,cpu_used_seconds,cpu_efficiency,memory_efficiency,max_rss_mb,billing_units,usage_source,efficiency_available", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("seff status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var report map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &report)
	if report["job_id"] != "job-eff" || report["job_name"] != "efficient" || report["state"] != models.SlurmStateCompleted || report["elapsed_seconds"].(float64) != 300 || report["alloc_cpus"].(float64) != 2 || report["alloc_mem"].(float64) != 1024 || report["allocated_cpu_seconds"].(float64) != 600 || report["cpu_used_seconds"].(float64) != 240 || report["cpu_efficiency"].(float64) != 40 || report["memory_efficiency"].(float64) != 50 || report["max_rss_mb"].(float64) != 512 || report["billing_units"].(float64) != 3 || report["usage_source"] != "srun_steps" || report["efficiency_available"] != true {
		t.Fatalf("unexpected seff report: %#v", report)
	}
}

func TestSlurmStriggerEndpoints(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	if err := db.Create(&models.AccountingRecord{
		CreatedAt:    time.Now(),
		SubmissionID: "job-trigger",
		UserID:       "u1",
		Account:      "course-a",
		Cluster:      "debug",
		Event:        database.AccountEventCompleted,
		State:        models.StatusSuccess,
		CPU:          2,
		Memory:       512,
		BillingUnits: 3,
	}).Error; err != nil {
		t.Fatalf("create accounting record: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/strigger?fields=trigger_id,name,event,job_id,active,program", `{"name":"on-end","event":"job_end","job_id":"job-trigger","program":"/bin/echo done"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("strigger create status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var trigger map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &trigger)
	triggerID := trigger["trigger_id"].(string)
	if triggerID == "" || trigger["name"] != "on-end" || trigger["event"] != "job_end" || trigger["job_id"] != "job-trigger" || trigger["active"] != true || trigger["program"] != "/bin/echo done" {
		t.Fatalf("unexpected created trigger: %#v", trigger)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/strigger?evaluate=true&name=on-end&fields=trigger_id,matched,match_count,active", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("strigger list status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var triggers []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &triggers)
	if len(triggers) != 1 || triggers[0]["trigger_id"] != triggerID || triggers[0]["matched"] != true || triggers[0]["match_count"].(float64) != 1 || triggers[0]["active"] != true {
		t.Fatalf("unexpected evaluated trigger list: %#v", triggers)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/strigger/evaluate?trigger_id="+triggerID+"&fields=trigger_id,matched,match_count,active,message", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("strigger evaluate status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &triggers)
	if len(triggers) != 1 || triggers[0]["matched"] != true || triggers[0]["match_count"].(float64) != 1 || triggers[0]["active"] != false {
		t.Fatalf("trigger should fire and deactivate: %#v", triggers)
	}

	recorder = performSlurmRequest(router, http.MethodDelete, "/api/v1/slurm/strigger/"+triggerID, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("strigger delete status = %d body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scontrol/update/node/debug/n1", `{"state":"down","reason":"maintenance"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("node update status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/strigger?fields=trigger_id,event,node,flags,active", `{"event":"node_down","node":"n1","flags":"keep-active"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("node trigger create status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &trigger)
	nodeTriggerID := trigger["trigger_id"].(string)
	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/strigger/evaluate?trigger_id="+nodeTriggerID+"&fields=trigger_id,matched,match_count,active", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("node trigger evaluate status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &triggers)
	if len(triggers) != 1 || triggers[0]["matched"] != true || triggers[0]["match_count"].(float64) != 1 || triggers[0]["active"] != true {
		t.Fatalf("keep-active node trigger should stay active: %#v", triggers)
	}
}

func TestSlurmScrontabEndpoints(t *testing.T) {
	router, db, scheduler := newSlurmTestRouterWithScheduler(t)
	pastRun := time.Now().Add(-time.Hour).Format(time.RFC3339)
	body, err := json.Marshal(map[string]interface{}{
		"name":        "periodic-p1",
		"schedule":    "@every 1h",
		"next_run_at": pastRun,
		"batch": map[string]interface{}{
			"user_id":    "u1",
			"problem_id": "p1",
			"partition":  "debug",
			"wrap":       "echo cron",
			"hold":       true,
		},
	})
	if err != nil {
		t.Fatalf("marshal scrontab body: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scrontab?fields=entry_id,name,enabled,user_id,problem_id,next_run_at", string(body))
	if recorder.Code != http.StatusOK {
		t.Fatalf("scrontab create status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var entry map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &entry)
	entryID := entry["entry_id"].(string)
	if entryID == "" || entry["name"] != "periodic-p1" || entry["enabled"] != true || entry["user_id"] != "u1" || entry["problem_id"] != "p1" || entry["next_run_at"] == nil {
		t.Fatalf("unexpected scrontab entry: %#v", entry)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scrontab?entry_id="+entryID+"&fields=entry_id,name,enabled,user_id,problem_id", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("scrontab list status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var entries []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &entries)
	if len(entries) != 1 || entries[0]["entry_id"] != entryID || entries[0]["enabled"] != true || entries[0]["user_id"] != "u1" || entries[0]["problem_id"] != "p1" {
		t.Fatalf("unexpected scrontab list: %#v", entries)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scrontab/evaluate?entry_id="+entryID+"&fields=entry_id,submitted,job_id,last_job_id,run_count,message", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("scrontab evaluate status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var evaluations []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &evaluations)
	if len(evaluations) != 1 || evaluations[0]["entry_id"] != entryID || evaluations[0]["submitted"] != true || evaluations[0]["run_count"].(float64) != 1 || evaluations[0]["message"] != "submitted" {
		t.Fatalf("unexpected scrontab evaluation: %#v", evaluations)
	}
	jobID := evaluations[0]["job_id"].(string)
	if jobID == "" || evaluations[0]["last_job_id"] != jobID {
		t.Fatalf("unexpected scrontab job ids: %#v", evaluations[0])
	}

	var sub models.Submission
	if err := db.First(&sub, "id = ?", jobID).Error; err != nil {
		t.Fatalf("load cron submission: %v", err)
	}
	if sub.UserID != "u1" || sub.ProblemID != "p1" || sub.Cluster != "debug" || sub.Status != models.StatusQueued || !sub.Hold || sub.Reason != "JobHeld" {
		t.Fatalf("unexpected cron submission: %#v", sub)
	}
	if lengths := scheduler.GetQueueLengths(); lengths["debug"] != 1 {
		t.Fatalf("queue lengths = %#v, want debug=1", lengths)
	}

	var submittedEvents int64
	if err := db.Model(&models.AccountingRecord{}).Where("submission_id = ? AND event = ?", jobID, database.AccountEventSubmitted).Count(&submittedEvents).Error; err != nil {
		t.Fatalf("count cron accounting records: %v", err)
	}
	if submittedEvents != 1 {
		t.Fatalf("cron submitted events = %d, want 1", submittedEvents)
	}

	recorder = performSlurmRequest(router, http.MethodDelete, "/api/v1/slurm/scrontab/"+entryID, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("scrontab delete status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var remaining int64
	if err := db.Model(&models.SlurmCronJob{}).Where("id = ?", entryID).Count(&remaining).Error; err != nil {
		t.Fatalf("count remaining cron jobs: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("remaining cron entries = %d, want 0", remaining)
	}
}

func TestSlurmDiagnosticsAndConfigEndpoints(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	if err := db.Create(&models.Submission{
		ID:        "job-diag",
		ProblemID: "p1",
		UserID:    "u1",
		Status:    models.StatusQueued,
		Cluster:   "debug",
		Account:   "course-a",
		QOS:       "normal",
	}).Error; err != nil {
		t.Fatalf("create diagnostic submission: %v", err)
	}
	if err := db.Create(&models.Allocation{
		ID:      "alloc-diag",
		Status:  models.AllocationActive,
		UserID:  "u1",
		Cluster: "debug",
		Node:    "n1",
		CPU:     1,
		Memory:  128,
	}).Error; err != nil {
		t.Fatalf("create diagnostic allocation: %v", err)
	}
	if err := db.Create(&models.RunStep{
		ID:           "step-diag",
		AllocationID: "alloc-diag",
		UserID:       "u1",
		Cluster:      "debug",
		Node:         "n1",
		Status:       models.StatusRunning,
		CPU:          1,
		Memory:       128,
		StartedAt:    time.Now(),
	}).Error; err != nil {
		t.Fatalf("create diagnostic run step: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sdiag?fields=active_jobs,jobs_by_state,nodes,total_cpus,licenses,allocations_by_state,steps_by_state,priority_weights", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("sdiag status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var diag map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &diag)
	if diag["active_jobs"].(float64) != 1 || diag["nodes"].(float64) != 1 || diag["total_cpus"].(float64) != 4 {
		t.Fatalf("unexpected sdiag totals: %#v", diag)
	}
	if diag["jobs_by_state"].(map[string]interface{})[models.SlurmStatePending].(float64) != 1 {
		t.Fatalf("unexpected sdiag job states: %#v", diag["jobs_by_state"])
	}
	if diag["allocations_by_state"].(map[string]interface{})[string(models.AllocationActive)].(float64) != 1 {
		t.Fatalf("unexpected sdiag allocation states: %#v", diag["allocations_by_state"])
	}
	if diag["steps_by_state"].(map[string]interface{})[string(models.StatusRunning)].(float64) != 1 {
		t.Fatalf("unexpected sdiag step states: %#v", diag["steps_by_state"])
	}
	licenses := diag["licenses"].([]interface{})
	if len(licenses) != 1 || licenses[0].(map[string]interface{})["license"] != "license/foo" || licenses[0].(map[string]interface{})["total"].(float64) != 2 {
		t.Fatalf("unexpected sdiag licenses: %#v", licenses)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/config?fields=queue_size,partitions,licenses,accounts,qos,priority_weights", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show config status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var cfg map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &cfg)
	if cfg["queue_size"].(float64) != 1024 {
		t.Fatalf("unexpected config queue_size: %#v", cfg)
	}
	if len(cfg["partitions"].([]interface{})) != 1 || len(cfg["accounts"].([]interface{})) != 2 || len(cfg["qos"].([]interface{})) != 1 {
		t.Fatalf("unexpected config lists: %#v", cfg)
	}
	if cfg["priority_weights"].(map[string]interface{})["partition"].(float64) != 100 {
		t.Fatalf("unexpected priority weights: %#v", cfg["priority_weights"])
	}
}

func TestSlurmSacctmgrAccountQOSAndAssociationEndpoints(t *testing.T) {
	router, _ := newSlurmTestRouter(t)

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/accounts?account=course-a&fields=account,fairshare,allowed_qos", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show accounts status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var accounts []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &accounts)
	if len(accounts) != 1 || accounts[0]["account"] != "course-a" || accounts[0]["fairshare"].(float64) != 20 {
		t.Fatalf("unexpected account response: %#v", accounts)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/sacctmgr/account", `{"account":"course-c","users":"u2,u3","qos":"debug,normal","parent_account":"root","fairshare":5,"max_jobs":2}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("create account status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var account map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &account)
	if account["account"] != "course-c" || account["parent_account"] != "root" || account["max_jobs"].(float64) != 2 {
		t.Fatalf("unexpected created account: %#v", account)
	}
	accountUsers := account["users"].([]interface{})
	allowedQOS := account["allowed_qos"].([]interface{})
	if len(accountUsers) != 2 || accountUsers[1] != "u3" || len(allowedQOS) != 2 || allowedQOS[0] != "debug" || allowedQOS[1] != "normal" {
		t.Fatalf("account string lists were not parsed: %#v", account)
	}

	recorder = performSlurmRequest(router, http.MethodPatch, "/api/v1/slurm/sacctmgr/account/course-c", `{"users":"u2 u3","allow_qos":"debug","fairshare":7,"max_jobs":3}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("update account status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &account)
	if account["account"] != "course-c" || account["fairshare"].(float64) != 7 || account["max_jobs"].(float64) != 3 {
		t.Fatalf("unexpected updated account: %#v", account)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/users?user=u1&fields=user,user_id,username,default_account,accounts,allowed_qos,association_count", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show users status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var users []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &users)
	if len(users) != 1 || users[0]["user"] != "u1" || users[0]["user_id"] != "u1" || users[0]["username"] != "alice" || users[0]["default_account"] != "course-a" || users[0]["association_count"].(float64) != 1 {
		t.Fatalf("unexpected user response: %#v", users)
	}
	userAccounts := users[0]["accounts"].([]interface{})
	userQOS := users[0]["allowed_qos"].([]interface{})
	if len(userAccounts) != 1 || userAccounts[0] != "course-a" || len(userQOS) != 1 || userQOS[0] != "normal" {
		t.Fatalf("unexpected user associations: %#v", users[0])
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/sacctmgr/user?fields=user,default_account,accounts,allowed_qos,association_count", `{"user":"u5","default_account":"course-c","qos":"debug,normal"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("create user status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var user map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &user)
	if user["user"] != "u5" || user["default_account"] != "course-c" || user["association_count"].(float64) != 2 {
		t.Fatalf("unexpected created user: %#v", user)
	}
	createdUserQOS := user["allowed_qos"].([]interface{})
	if len(createdUserQOS) != 2 || createdUserQOS[0] != "debug" || createdUserQOS[1] != "normal" {
		t.Fatalf("unexpected created user qos: %#v", user)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/assoc?account=course-c&user=u5&qos=normal&fields=account,user,qos", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show user assoc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var userAssociations []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &userAssociations)
	if len(userAssociations) != 1 || userAssociations[0]["user"] != "u5" || userAssociations[0]["qos"] != "normal" {
		t.Fatalf("unexpected user associations: %#v", userAssociations)
	}

	recorder = performSlurmRequest(router, http.MethodDelete, "/api/v1/slurm/sacctmgr/user/u5?account=course-c", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("delete user status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/users?account=course-c&user=u5", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show deleted user status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &users)
	if len(users) != 0 {
		t.Fatalf("user should be deleted from account: %#v", users)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/sacctmgr/qos", `{"qos":"debug","priority":9,"max_cpu_per_job":2,"max_memory_per_job":"2G","max_time":"01:30:00","preempt":"normal,low"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("create qos status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var qos map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &qos)
	if qos["qos"] != "debug" || qos["priority"].(float64) != 9 || qos["max_cpu_per_job"].(float64) != 2 || qos["max_memory_per_job"].(float64) != 2048 || qos["max_time"].(float64) != 90*60 {
		t.Fatalf("unexpected qos: %#v", qos)
	}
	preempt := qos["preempt"].([]interface{})
	if len(preempt) != 2 || preempt[0] != "normal" || preempt[1] != "low" {
		t.Fatalf("qos preempt list was not parsed: %#v", qos)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/assoc?account=course-c&user=u2&qos=debug&fields=account,user,qos,fairshare,max_jobs", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show assoc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var associations []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &associations)
	if len(associations) != 1 || associations[0]["account"] != "course-c" || associations[0]["user"] != "u2" || associations[0]["qos"] != "debug" || associations[0]["fairshare"].(float64) != 7 {
		t.Fatalf("unexpected associations: %#v", associations)
	}

	recorder = performSlurmRequest(router, http.MethodPatch, "/api/v1/slurm/sacctmgr/assoc/course-c", `{"user_id":"u4","qos":"debug","fairshare":8,"max_jobs":4}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("upsert assoc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var association map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &association)
	if association["account"] != "course-c" || association["user"] != "u4" || association["qos"] != "debug" || association["fairshare"].(float64) != 8 || association["max_jobs"].(float64) != 4 {
		t.Fatalf("unexpected upserted association: %#v", association)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/assoc?account=course-c&user=u4&qos=debug&fields=account,user,qos,fairshare,max_jobs", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show upserted assoc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &associations)
	if len(associations) != 1 || associations[0]["user"] != "u4" || associations[0]["fairshare"].(float64) != 8 || associations[0]["max_jobs"].(float64) != 4 {
		t.Fatalf("unexpected upserted associations: %#v", associations)
	}

	recorder = performSlurmRequest(router, http.MethodDelete, "/api/v1/slurm/sacctmgr/assoc/course-c?user=u4", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("delete assoc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/assoc?account=course-c&user=u4", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show deleted assoc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &associations)
	if len(associations) != 0 {
		t.Fatalf("association should be deleted: %#v", associations)
	}

	recorder = performSlurmRequest(router, http.MethodDelete, "/api/v1/slurm/sacctmgr/qos/debug", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("delete qos status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = performSlurmRequest(router, http.MethodDelete, "/api/v1/slurm/sacctmgr/account/course-c", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("delete account status = %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestSlurmSacctmgrShowTRESEndpoint(t *testing.T) {
	router, _ := newSlurmTestRouterWithNodes(t, []config.Node{
		{Name: "n1", CPU: 4, Memory: 1024, GRES: []string{"gpu:2"}},
		{Name: "n2", CPU: 2, Memory: 2048, GRES: []string{"gpu:1", "fpga:1"}},
	})

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/tres?fields=tres,type,name,count,billing_weight,source", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show tres status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var records []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &records)
	byTRES := make(map[string]map[string]interface{})
	for _, record := range records {
		byTRES[record["tres"].(string)] = record
	}
	for _, tres := range []string{"cpu", "mem", "node", "gres/gpu", "gres/fpga", "license/foo"} {
		if byTRES[tres] == nil {
			t.Fatalf("missing tres %s in %#v", tres, records)
		}
	}
	if byTRES["cpu"]["count"].(float64) != 6 || byTRES["mem"]["count"].(float64) != 3072 || byTRES["node"]["count"].(float64) != 2 || byTRES["gres/gpu"]["count"].(float64) != 3 || byTRES["gres/fpga"]["count"].(float64) != 1 || byTRES["license/foo"]["count"].(float64) != 2 {
		t.Fatalf("unexpected tres counts: %#v", byTRES)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/tres?type=gres&fields=tres,type,count", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("filter tres status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &records)
	if len(records) != 2 {
		t.Fatalf("gres records len = %d, want 2: %#v", len(records), records)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/tres?tres=license/foo&fields=tres,type,name,count", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("license tres filter status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &records)
	if len(records) != 1 || records[0]["tres"] != "license/foo" || records[0]["type"] != "license" || records[0]["name"] != "foo" || records[0]["count"].(float64) != 2 {
		t.Fatalf("unexpected license tres record: %#v", records)
	}
}

func TestSlurmSacctmgrShowClustersEndpoint(t *testing.T) {
	router, _ := newSlurmTestRouterWithNodes(t, []config.Node{
		{Name: "n1", CPU: 4, Memory: 1024, Features: []string{"gpu", "avx"}, GRES: []string{"gpu:2"}},
		{Name: "n2", CPU: 2, Memory: 2048, Features: []string{"avx"}, Runtime: judger.RuntimeKubernetes},
	})

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/cluster?cluster=debug&fields=partition,state,node_count,total_cpus,total_memory,tres,features,runtimes,queue_length,account_count,qos_count,license_count", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show clusters status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var records []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &records)
	if len(records) != 1 {
		t.Fatalf("clusters len = %d, want 1: %#v", len(records), records)
	}
	record := records[0]
	if record["partition"] != "debug" || record["state"] != "UP" || record["node_count"].(float64) != 2 || record["total_cpus"].(float64) != 6 || record["total_memory"].(float64) != 3072 || record["queue_length"].(float64) != 0 || record["account_count"].(float64) != 2 || record["qos_count"].(float64) != 1 || record["license_count"].(float64) != 1 {
		t.Fatalf("unexpected cluster record: %#v", record)
	}
	tres := record["tres"].(string)
	for _, expected := range []string{"cpu=6", "mem=3072M", "node=2", "gres/gpu=2"} {
		if !strings.Contains(tres, expected) {
			t.Fatalf("cluster tres %q missing %s", tres, expected)
		}
	}
	features := record["features"].([]interface{})
	if len(features) != 2 || features[0] != "avx" || features[1] != "gpu" {
		t.Fatalf("unexpected cluster features: %#v", features)
	}
	runtimes := record["runtimes"].([]interface{})
	if len(runtimes) != 2 || runtimes[0] != judger.RuntimeDocker || runtimes[1] != judger.RuntimeKubernetes {
		t.Fatalf("unexpected cluster runtimes: %#v", runtimes)
	}
}

func TestSlurmScontrolPingAndShowDaemonsEndpoints(t *testing.T) {
	router, _ := newSlurmTestRouterWithNodes(t, []config.Node{
		{Name: "n1", CPU: 4, Memory: 1024},
		{Name: "n2", CPU: 2, Memory: 2048, State: "down", Reason: "maintenance"},
	})

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/daemons?daemon=slurmd&fields=daemon,node,state,status,responding,runtime,message", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show daemons status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var daemons []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &daemons)
	if len(daemons) != 2 {
		t.Fatalf("slurmd records len = %d, want 2: %#v", len(daemons), daemons)
	}
	if daemons[0]["daemon"] != "slurmd" || daemons[0]["node"] != "n1" || daemons[0]["status"] != "UP" || daemons[0]["responding"] != true {
		t.Fatalf("unexpected first daemon: %#v", daemons[0])
	}
	if daemons[1]["node"] != "n2" || daemons[1]["state"] != "DOWN" || daemons[1]["status"] != "DOWN" || daemons[1]["responding"] != false || daemons[1]["message"] != "maintenance" {
		t.Fatalf("unexpected down daemon: %#v", daemons[1])
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/ping?fields=responding,status,controller_count,cluster_count,daemon_count,controllers", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("scontrol ping status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var ping map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &ping)
	if ping["responding"] != true || ping["status"] != "UP" || ping["controller_count"].(float64) != 3 || ping["cluster_count"].(float64) != 1 || ping["daemon_count"].(float64) != 5 {
		t.Fatalf("unexpected ping response: %#v", ping)
	}
	controllers := ping["controllers"].([]interface{})
	if len(controllers) != 3 {
		t.Fatalf("controllers len = %d, want 3: %#v", len(controllers), controllers)
	}
	for _, controller := range controllers {
		if controller.(map[string]interface{})["responding"] != true {
			t.Fatalf("controller should be responding: %#v", controller)
		}
	}
}

func TestSlurmSacctmgrPingEndpoint(t *testing.T) {
	router, _ := newSlurmTestRouter(t)

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/ping?fields=daemon,service,status,responding,primary,message", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("sacctmgr ping status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var ping map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &ping)
	if ping["daemon"] != "slurmdbd" || ping["service"] != "database" || ping["status"] != "UP" || ping["responding"] != true || ping["primary"] != true || ping["message"] != "database ping ok" {
		t.Fatalf("unexpected sacctmgr ping response: %#v", ping)
	}
}

func TestSlurmSacctmgrAccountingViewEndpoints(t *testing.T) {
	router, db := newSlurmTestRouterWithNodes(t, []config.Node{
		{Name: "n1", CPU: 4, Memory: 1024, GRES: []string{"gpu:1"}},
		{Name: "n2", CPU: 2, Memory: 2048, State: "down", Reason: "maintenance"},
	})
	now := time.Now().UTC().Truncate(time.Second)
	if err := db.Create(&[]models.Submission{
		{ID: "job-done", CreatedAt: now.Add(-10 * time.Minute), ProblemID: "p1", UserID: "u1", JobName: "done", Status: models.StatusSuccess, Cluster: "debug", Node: "n1", CPU: 2, Memory: 512, Account: "course-a", QOS: "normal"},
		{ID: "job-run", CreatedAt: now.Add(-5 * time.Minute), ProblemID: "p1", UserID: "u1", JobName: "run", Status: models.StatusRunning, Cluster: "debug", Node: "n1", CPU: 1, Memory: 256, Account: "course-a", QOS: "normal"},
	}).Error; err != nil {
		t.Fatalf("create submissions: %v", err)
	}
	accounting := []models.AccountingRecord{
		{CreatedAt: now.Add(-9 * time.Minute), SubmissionID: "job-done", UserID: "u1", ProblemID: "p1", JobName: "done", Cluster: "debug", Node: "n1", Account: "course-a", QOS: "normal", Event: database.AccountEventSubmitted, State: models.StatusQueued},
		{CreatedAt: now.Add(-8 * time.Minute), SubmissionID: "job-done", UserID: "u1", ProblemID: "p1", JobName: "done", Cluster: "debug", Node: "n1", Account: "course-a", QOS: "normal", Event: database.AccountEventStarted, State: models.StatusRunning, CPU: 2, Memory: 512, BillingUnits: 3},
		{CreatedAt: now.Add(-7 * time.Minute), SubmissionID: "job-done", UserID: "u1", ProblemID: "p1", JobName: "done", Cluster: "debug", Node: "n1", Account: "course-a", QOS: "normal", Event: database.AccountEventCompleted, State: models.StatusSuccess, CPU: 2, Memory: 512, BillingUnits: 3},
		{CreatedAt: now.Add(-4 * time.Minute), SubmissionID: "job-run", UserID: "u1", ProblemID: "p1", JobName: "run", Cluster: "debug", Node: "n1", Account: "course-a", QOS: "normal", Event: database.AccountEventStarted, State: models.StatusRunning, CPU: 1, Memory: 256, BillingUnits: 1},
	}
	for i := range accounting {
		if err := db.Create(&accounting[i]).Error; err != nil {
			t.Fatalf("create accounting record: %v", err)
		}
	}
	if err := db.Create(&models.Allocation{
		ID:        "alloc-active",
		CreatedAt: now.Add(-3 * time.Minute),
		Status:    models.AllocationActive,
		UserID:    "u1",
		Cluster:   "debug",
		Node:      "n1",
		CPU:       1,
		Memory:    128,
		Account:   "course-a",
		QOS:       "normal",
	}).Error; err != nil {
		t.Fatalf("create allocation: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/config?fields=database_status,cluster_count,account_count,qos_count", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show config status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var configView map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &configView)
	if configView["database_status"] != "UP" || configView["cluster_count"].(float64) != 1 || configView["account_count"].(float64) != 2 || configView["qos_count"].(float64) != 1 {
		t.Fatalf("unexpected sacctmgr config: %#v", configView)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/stats?fields=jobs,accounting_records,allocations,accounting_by_event", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show stats status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var stats map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &stats)
	if stats["jobs"].(float64) != 2 || stats["accounting_records"].(float64) != 4 || stats["allocations"].(float64) != 1 {
		t.Fatalf("unexpected sacctmgr stats: %#v", stats)
	}
	if stats["accounting_by_event"].(map[string]interface{})[database.AccountEventStarted].(float64) != 2 {
		t.Fatalf("unexpected accounting event stats: %#v", stats["accounting_by_event"])
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/jobs?job_id=job-done&fields=job_id,state,accounting_records,last_event,terminal_accounting,elapsed_seconds,alloc_cpus,alloc_mem", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show jobs status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var jobs []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &jobs)
	if len(jobs) != 1 || jobs[0]["job_id"] != "job-done" || jobs[0]["state"] != models.SlurmStateCompleted || jobs[0]["accounting_records"].(float64) != 3 || jobs[0]["last_event"] != database.AccountEventCompleted || jobs[0]["terminal_accounting"] != true || jobs[0]["elapsed_seconds"].(float64) != 60 || jobs[0]["alloc_cpus"].(float64) != 2 || jobs[0]["alloc_mem"].(float64) != 512 {
		t.Fatalf("unexpected sacctmgr job view: %#v", jobs)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/problems?problem_id=p1&fields=problem_id,state,submissions,accounting_records,cpus,memory", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show problems status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var problems []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &problems)
	if len(problems) != 1 || problems[0]["problem_id"] != "p1" || problems[0]["state"] != "ACTIVE" || problems[0]["submissions"].(float64) != 2 || problems[0]["accounting_records"].(float64) != 4 || problems[0]["cpus"].(float64) != 1 || problems[0]["memory"].(float64) != 128 {
		t.Fatalf("unexpected sacctmgr problem view: %#v", problems)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/resources?type=license&fields=resource,type,count,state", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show resources status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var resources []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &resources)
	if len(resources) != 1 || resources[0]["resource"] != "license/foo" || resources[0]["type"] != "license" || resources[0]["count"].(float64) != 2 || resources[0]["state"] != "ACTIVE" {
		t.Fatalf("unexpected sacctmgr resources: %#v", resources)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/runawayjobs?user=u1&fields=kind,job_id,state,runaway_candidate,candidate_reason", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show runaway jobs status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var runaway []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &runaway)
	seenRunaway := make(map[string]map[string]interface{})
	for _, record := range runaway {
		seenRunaway[record["job_id"].(string)] = record
	}
	if len(runaway) != 2 || seenRunaway["job-run"] == nil || seenRunaway["alloc-active"] == nil || seenRunaway["job-run"]["runaway_candidate"] != true || seenRunaway["alloc-active"]["kind"] != "allocation" {
		t.Fatalf("unexpected runaway job records: %#v", runaway)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/transactions?action=Started&job_id=job-run&fields=transaction_id,action,job_id,state", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show transactions status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var transactions []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &transactions)
	if len(transactions) != 1 || transactions[0]["action"] != database.AccountEventStarted || transactions[0]["job_id"] != "job-run" || transactions[0]["state"] != models.SlurmStateRunning {
		t.Fatalf("unexpected transaction records: %#v", transactions)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sacctmgr/show/events?node=n2&fields=source,event,node,state,reason", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show events status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var events []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &events)
	if len(events) != 1 || events[0]["source"] != "scheduler_node" || events[0]["event"] != "NodeDown" || events[0]["node"] != "n2" || events[0]["state"] != "DOWN" || events[0]["reason"] != "maintenance" {
		t.Fatalf("unexpected event records: %#v", events)
	}
}

func TestSlurmReservationManagementEndpoints(t *testing.T) {
	router, _ := newSlurmTestRouter(t)
	start := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	end := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	body := `{"name":"debug-res","cluster":"debug","nodes":["n1"],"users":["alice"],"accounts":["course-a"],"starttime":"` + start + `","endtime":"` + end + `","cpu":2,"memory":512,"allow_overlap":true}`

	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scontrol/create/reservation", body)
	if recorder.Code != http.StatusOK {
		t.Fatalf("create reservation status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var reservation map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &reservation)
	if reservation["reservation"] != "debug-res" || reservation["partition"] != "debug" || reservation["state"] != "ACTIVE" || reservation["cpu"].(float64) != 2 {
		t.Fatalf("unexpected reservation: %#v", reservation)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/reservations?reservation=debug-res&fields=reservation,state,partition,cpu", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show reservations status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var reservations []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &reservations)
	if len(reservations) != 1 || reservations[0]["reservation"] != "debug-res" || reservations[0]["state"] != "ACTIVE" {
		t.Fatalf("unexpected reservations: %#v", reservations)
	}

	aliasStart := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	aliasBody := `{"reservation":"alias-res","partition":"debug","nodes":"n1,n2","users":"alice,bob","accounts":"course-a course-b","start_time":"` + aliasStart.Format(time.RFC3339) + `","duration":"01:00:00","cpu":1,"mem":"1G","ignore_running":true}`
	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scontrol/create/reservation", aliasBody)
	if recorder.Code != http.StatusOK {
		t.Fatalf("create alias reservation status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &reservation)
	if reservation["reservation"] != "alias-res" || reservation["partition"] != "debug" || reservation["memory"].(float64) != 1024 || reservation["ignore_running"] != true {
		t.Fatalf("unexpected alias reservation: %#v", reservation)
	}
	nodes := reservation["nodes"].([]interface{})
	users := reservation["users"].([]interface{})
	accounts := reservation["accounts"].([]interface{})
	if len(nodes) != 2 || nodes[0] != "n1" || nodes[1] != "n2" || len(users) != 2 || users[1] != "bob" || len(accounts) != 2 || accounts[1] != "course-b" {
		t.Fatalf("reservation string lists were not parsed: %#v", reservation)
	}
	endTime, err := time.Parse(time.RFC3339, reservation["end_time"].(string))
	if err != nil {
		t.Fatalf("parse alias reservation end_time: %v", err)
	}
	if endTime.Sub(aliasStart) != time.Hour || reservation["duration"].(float64) != float64(time.Hour/time.Second) {
		t.Fatalf("duration alias not applied: %#v", reservation)
	}

	recorder = performSlurmRequest(router, http.MethodDelete, "/api/v1/slurm/scontrol/delete/reservation/debug-res", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("delete reservation status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/scontrol/show/reservations?reservation=debug-res", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show deleted reservation status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &reservations)
	if len(reservations) != 0 {
		t.Fatalf("reservation should be deleted: %#v", reservations)
	}
}

func TestSlurmSallocAllocatesAndReleasesResources(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user":"u1","cluster":"debug","cpus":2,"memory":"2G","time_limit":"2-03:04","tres":"license/foo:1"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("salloc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var allocation map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &allocation)
	allocationID := allocation["allocation_id"].(string)
	if allocationID == "" || allocation["state"] != "ALLOCATED" || allocation["node"] != "n1" || allocation["memory"].(float64) != 2048 || allocation["time_limit"].(float64) != 2*24*3600+3*3600+4*60 {
		t.Fatalf("unexpected allocation response: %#v", allocation)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user_id":"u1","partition":"debug","cpus":1,"memory":128,"tres":"license/foo:2"}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("second salloc should fail because license pool is exhausted enough, status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc/"+allocationID+"/release", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("release status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &allocation)
	if allocation["state"] != "RELEASED" {
		t.Fatalf("unexpected release response: %#v", allocation)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user_id":"u1","partition":"debug","cpus":1,"memory":128,"tres":"license/foo:2"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("salloc after release should succeed, status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	var events int64
	if err := db.Model(&models.AccountingRecord{}).Where("submission_id = ? AND event IN ?", allocationID, []string{database.AccountEventAllocated, database.AccountEventAllocationReleased}).Count(&events).Error; err != nil {
		t.Fatalf("count allocation accounting: %v", err)
	}
	if events != 2 {
		t.Fatalf("allocation accounting events = %d, want 2", events)
	}
}

func TestSlurmSallocAllocatesMultiNodeResources(t *testing.T) {
	router, db := newSlurmTestRouterWithNodes(t, []config.Node{
		{Name: "n1", CPU: 4, Memory: 4096},
		{Name: "n2", CPU: 4, Memory: 4096},
	})

	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user_id":"u1","partition":"debug","cpus":3,"memory":256,"nodes":2}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("multi-node salloc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var allocation map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &allocation)
	allocationID := allocation["allocation_id"].(string)
	if allocationID == "" || allocation["state"] != "ALLOCATED" || allocation["node"] != "n1,n2" || allocation["nodes"].(float64) != 2 || allocation["cpus"].(float64) != 3 || allocation["allocated_cores"] != "0,1" || allocation["allocated_node_cores"] != "n1:0,1;n2:0" {
		t.Fatalf("unexpected multi-node allocation response: %#v", allocation)
	}
	env := allocation["env"].(map[string]interface{})
	if env["SLURM_JOB_NUM_NODES"] != "2" || env["SLURM_NNODES"] != "2" || env["SLURM_CPUS_ON_NODE"] != "2" || env["SLURM_JOB_CPUS_PER_NODE"] != "2,1" {
		t.Fatalf("unexpected multi-node allocation env: %#v", env)
	}

	var stored models.Allocation
	if err := db.First(&stored, "id = ?", allocationID).Error; err != nil {
		t.Fatalf("load allocation: %v", err)
	}
	if stored.Node != "n1,n2" || stored.Nodes != 2 || stored.AllocatedNodeCores != "n1:0,1;n2:0" {
		t.Fatalf("unexpected stored allocation: %#v", stored)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc/"+allocationID+"/release", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("release multi-node allocation status = %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestSlurmSallocHonorsNodeListAndExclude(t *testing.T) {
	router, db := newSlurmTestRouterWithNodes(t, []config.Node{
		{Name: "n1", CPU: 4, Memory: 4096},
		{Name: "n2", CPU: 4, Memory: 4096},
		{Name: "n3", CPU: 4, Memory: 4096},
	})

	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user_id":"u1","partition":"debug","cpus":1,"memory":128,"nodeslist":"n[2-3]","exclude":"n3"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("node-filtered salloc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var allocation map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &allocation)
	allocationID := allocation["allocation_id"].(string)
	if allocation["node"] != "n2" || allocation["requested_nodelist"] != "n[2-3]" || allocation["exclude_nodes"] != "n3" {
		t.Fatalf("unexpected node-filtered allocation: %#v", allocation)
	}

	var stored models.Allocation
	if err := db.First(&stored, "id = ?", allocationID).Error; err != nil {
		t.Fatalf("load allocation: %v", err)
	}
	if stored.Node != "n2" || stored.NodeList != "n[2-3]" || stored.ExcludeNodes != "n3" {
		t.Fatalf("unexpected stored allocation filters: %#v", stored)
	}
}

func TestSlurmSallocExclusiveReservesWholeNode(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user_id":"u1","partition":"debug","cpus":1,"memory":128,"exclusive":true}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("exclusive salloc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var allocation map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &allocation)
	allocationID := allocation["allocation_id"].(string)
	if allocation["exclusive"] != true || allocation["allocated_cores"] != "0,1,2,3" {
		t.Fatalf("exclusive allocation should reserve every core: %#v", allocation)
	}

	var stored models.Allocation
	if err := db.First(&stored, "id = ?", allocationID).Error; err != nil {
		t.Fatalf("load allocation: %v", err)
	}
	if !stored.Exclusive || stored.AllocatedCores != "0,1,2,3" {
		t.Fatalf("exclusive allocation was not persisted: %#v", stored)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user_id":"u1","partition":"debug","cpus":1,"memory":128}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("shared salloc should not share an exclusive node, status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc/"+allocationID+"/release", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("release exclusive allocation status = %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestSlurmSrunExecutesStepAndRecordsAccounting(t *testing.T) {
	router, db, scheduler := newSlurmTestRouterWithScheduler(t)
	runtime := &fakeRuntime{
		stdout: "hello\n",
		usage:  judger.RuntimeUsage{AveCPU: 1.25, AveRSS: 1024, MaxRSS: 2048, MaxVMSize: 4096},
	}
	scheduler.SetRuntimeFactory(func(config.Node) (judger.RuntimeManager, error) {
		return runtime, nil
	})

	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user_id":"u1","partition":"debug","cpus":2,"memory":512,"tres":"license/foo:1"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("salloc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var allocation map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &allocation)
	allocationID := allocation["allocation_id"].(string)

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/sbcast", `{"allocation_id":"`+allocationID+`","path":"input/config.txt","content":"cfg=1\n","files_base64":{"bin/token.txt":"dG9rZW4K"}}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("sbcast status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var broadcast map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &broadcast)
	if broadcast["allocation_id"] != allocationID || broadcast["state"] != "BROADCASTED" || broadcast["file_count"].(float64) != 2 || broadcast["bytes"].(float64) != 12 {
		t.Fatalf("unexpected sbcast response: %#v", broadcast)
	}
	broadcastDir := broadcast["staging_dir"].(string)
	configData, err := os.ReadFile(filepath.Join(broadcastDir, "mnt", "work", "input", "config.txt"))
	if err != nil {
		t.Fatalf("read staged config: %v", err)
	}
	tokenData, err := os.ReadFile(filepath.Join(broadcastDir, "mnt", "work", "bin", "token.txt"))
	if err != nil {
		t.Fatalf("read staged token: %v", err)
	}
	if string(configData) != "cfg=1\n" || string(tokenData) != "token\n" {
		t.Fatalf("unexpected staged sbcast files config=%q token=%q", string(configData), string(tokenData))
	}

	expectedStepCores := strings.Split(allocation["allocated_cores"].(string), ",")[0]
	body := `{"allocation_id":"` + allocationID + `","command_line":"echo hello","image":"busybox:latest","time":"00:05","cpus":1,"memory":"128M","network":true}`
	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/srun", body)
	if recorder.Code != http.StatusOK {
		t.Fatalf("srun status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var step map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &step)
	stepID := step["step_id"].(string)
	if stepID == "" || step["allocation_id"] != allocationID || step["state"] != models.SlurmStateCompleted || step["stdout"] != "hello\n" {
		t.Fatalf("unexpected srun response: %#v", step)
	}
	if step["cpus"] != float64(1) || step["memory"] != float64(128) || step["timeout"] != float64(5) || step["allocated_cores"] != expectedStepCores {
		t.Fatalf("unexpected srun resources: %#v", step)
	}
	if len(runtime.createdVolumes) != 1 || runtime.createdVolumes[0] != allocationID {
		t.Fatalf("created volumes = %#v, want allocation volume", runtime.createdVolumes)
	}
	if len(runtime.createdCPUs) != 1 || runtime.createdCPUs[0] != 1 || len(runtime.createdMemory) != 1 || runtime.createdMemory[0] != 128 || len(runtime.createdCpuset) != 1 || runtime.createdCpuset[0] != expectedStepCores {
		t.Fatalf("runtime resources cpu=%#v memory=%#v cpuset=%#v", runtime.createdCPUs, runtime.createdMemory, runtime.createdCpuset)
	}
	if len(runtime.copiedSrcDirs) != 1 || runtime.copiedSrcDirs[0] != broadcastDir || len(runtime.copiedDstDirs) != 1 || runtime.copiedDstDirs[0] != "/" {
		t.Fatalf("sbcast copy calls src=%#v dst=%#v", runtime.copiedSrcDirs, runtime.copiedDstDirs)
	}
	if len(runtime.execCommands) != 1 || strings.Join(runtime.execCommands[0], " ") != "/bin/sh -lc echo hello" {
		t.Fatalf("exec commands = %#v", runtime.execCommands)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/srun?job_id="+allocationID+"&states=CD&fields=step_id,state", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("list srun status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var steps []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &steps)
	if len(steps) != 1 || steps[0]["step_id"] != stepID || steps[0]["state"] != models.SlurmStateCompleted {
		t.Fatalf("unexpected srun list: %#v", steps)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sstat?allocation_id="+allocationID+"&fields=step_id,state,alloc_cpus,alloc_memory,allocated_cores,ave_cpu,ave_rss,max_rss,max_vmsize", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("sstat status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var stats []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &stats)
	if len(stats) != 1 || stats[0]["step_id"] != stepID || stats[0]["state"] != models.SlurmStateCompleted || stats[0]["alloc_cpus"] != float64(1) || stats[0]["alloc_memory"] != float64(128) || stats[0]["allocated_cores"] != expectedStepCores {
		t.Fatalf("unexpected sstat list: %#v", stats)
	}
	if stats[0]["ave_cpu"] != 1.25 || stats[0]["ave_rss"] != float64(1024) || stats[0]["max_rss"] != float64(2048) || stats[0]["max_vmsize"] != float64(4096) {
		t.Fatalf("unexpected sstat usage counters: %#v", stats[0])
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/srun/"+stepID, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show srun status = %d body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sattach/"+stepID+"?fields=step_id,job_step_id,job_id,state,stdout,stderr,stdout_bytes,stderr_bytes,attached,stdin_supported", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("sattach status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var attached map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &attached)
	if attached["step_id"] != stepID || attached["job_id"] != allocationID || attached["job_step_id"] != allocationID+"."+stepID || attached["state"] != models.SlurmStateCompleted || attached["stdout"] != "hello\n" || attached["stdout_bytes"].(float64) != 6 || attached["attached"] != true || attached["stdin_supported"] != false {
		t.Fatalf("unexpected sattach response: %#v", attached)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sattach?job_id="+allocationID+"&states=CD&fields=step_id,stdout", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("sattach list status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &steps)
	if len(steps) != 1 || steps[0]["step_id"] != stepID || steps[0]["stdout"] != "hello\n" {
		t.Fatalf("unexpected sattach list: %#v", steps)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/sstat/"+stepID, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("show sstat status = %d body=%s", recorder.Code, recorder.Body.String())
	}

	var events int64
	if err := db.Model(&models.AccountingRecord{}).Where("submission_id = ? AND event IN ?", allocationID, []string{database.AccountEventRunStarted, database.AccountEventRunCompleted}).Count(&events).Error; err != nil {
		t.Fatalf("count srun accounting: %v", err)
	}
	if events != 2 {
		t.Fatalf("srun accounting events = %d, want 2", events)
	}
	var runRecord models.AccountingRecord
	if err := db.Where("submission_id = ? AND event = ?", allocationID, database.AccountEventRunStarted).First(&runRecord).Error; err != nil {
		t.Fatalf("find run accounting record: %v", err)
	}
	if runRecord.CPU != 1 || runRecord.Memory != 128 {
		t.Fatalf("run accounting resources cpu=%d memory=%d, want 1/128", runRecord.CPU, runRecord.Memory)
	}

	busyStep := models.RunStep{
		ID:             "step-busy",
		AllocationID:   allocationID,
		UserID:         "u1",
		Cluster:        "debug",
		Node:           allocation["node"].(string),
		Status:         models.StatusRunning,
		CPU:            2,
		Memory:         512,
		AllocatedCores: allocation["allocated_cores"].(string),
		StartedAt:      time.Now(),
	}
	if err := db.Create(&busyStep).Error; err != nil {
		t.Fatalf("create busy step: %v", err)
	}
	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/srun", `{"allocation_id":"`+allocationID+`","command_line":"echo blocked","cpus":1,"memory":1}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("oversubscribed srun status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if err := db.Delete(&models.RunStep{}, "id = ?", busyStep.ID).Error; err != nil {
		t.Fatalf("delete busy step: %v", err)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc/"+allocationID+"/release", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("release status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if _, err := os.Stat(broadcastDir); !os.IsNotExist(err) {
		t.Fatalf("sbcast staging dir should be removed on release, err=%v", err)
	}
	if len(runtime.removedVolumes) != 1 || runtime.removedVolumes[0] != allocationID {
		t.Fatalf("removed volumes = %#v, want allocation volume", runtime.removedVolumes)
	}
}

func TestSlurmSrunCreatesImplicitAllocation(t *testing.T) {
	router, db, scheduler := newSlurmTestRouterWithScheduler(t)
	runtime := &fakeRuntime{stdout: "implicit\n"}
	scheduler.SetRuntimeFactory(func(config.Node) (judger.RuntimeManager, error) {
		return runtime, nil
	})

	body := `{"user_id":"u1","partition":"debug","command_line":"echo implicit","image":"busybox:latest","cpus":2,"memory":256,"time":"00:10","tres":"license/foo:1","nodelist":"n1"}`
	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/srun", body)
	if recorder.Code != http.StatusOK {
		t.Fatalf("implicit srun status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var step map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &step)
	allocationID := step["allocation_id"].(string)
	if allocationID == "" || step["state"] != models.SlurmStateCompleted || step["stdout"] != "implicit\n" || step["implicit_allocation"] != true || step["allocation_released"] != true {
		t.Fatalf("unexpected implicit srun response: %#v", step)
	}
	if step["cpus"] != float64(2) || step["memory"] != float64(256) || step["timeout"] != float64(10) {
		t.Fatalf("unexpected implicit srun resources: %#v", step)
	}
	if len(runtime.createdVolumes) != 1 || runtime.createdVolumes[0] != allocationID || len(runtime.removedVolumes) != 1 || runtime.removedVolumes[0] != allocationID {
		t.Fatalf("implicit allocation volume lifecycle created=%#v removed=%#v", runtime.createdVolumes, runtime.removedVolumes)
	}

	var allocation models.Allocation
	if err := db.First(&allocation, "id = ?", allocationID).Error; err != nil {
		t.Fatalf("load implicit allocation: %v", err)
	}
	if allocation.Status != models.AllocationReleased || allocation.CPU != 2 || allocation.Memory != 256 || allocation.TRES != "license/foo:1" || allocation.NodeList != "n1" {
		t.Fatalf("unexpected implicit allocation record: %#v", allocation)
	}

	var events int64
	if err := db.Model(&models.AccountingRecord{}).Where("submission_id = ? AND event IN ?", allocationID, []string{database.AccountEventAllocated, database.AccountEventRunStarted, database.AccountEventRunCompleted, database.AccountEventAllocationReleased}).Count(&events).Error; err != nil {
		t.Fatalf("count implicit srun accounting: %v", err)
	}
	if events != 4 {
		t.Fatalf("implicit srun accounting events = %d, want 4", events)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user_id":"u1","partition":"debug","cpus":1,"memory":128,"tres":"license/foo:2"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("license pool should be released after implicit srun, status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestSlurmSrunImplicitAllocationRequiresUser(t *testing.T) {
	router, _ := newSlurmTestRouter(t)
	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/srun", `{"command_line":"echo no-user","cpus":1}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("implicit srun without user should fail, status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestSlurmSrunHonorsAllocationTimeLimit(t *testing.T) {
	router, db, scheduler := newSlurmTestRouterWithScheduler(t)
	runtime := &fakeRuntime{stdout: "ok\n"}
	scheduler.SetRuntimeFactory(func(config.Node) (judger.RuntimeManager, error) {
		return runtime, nil
	})

	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user_id":"u1","partition":"debug","cpus":1,"memory":128,"time_limit":60}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("salloc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var allocation map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &allocation)
	allocationID := allocation["allocation_id"].(string)
	nearDeadline := time.Now().Add(-55 * time.Second)
	if err := db.Model(&models.Allocation{}).Where("id = ?", allocationID).Update("created_at", nearDeadline).Error; err != nil {
		t.Fatalf("move allocation time: %v", err)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/srun", `{"allocation_id":"`+allocationID+`","command_line":"echo ok","time_limit":120}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("srun status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if len(runtime.execDeadlines) != 1 || runtime.execDeadlines[0] <= 0 || runtime.execDeadlines[0] > 6*time.Second {
		t.Fatalf("srun deadline should be capped by allocation remaining time, got %#v", runtime.execDeadlines)
	}
	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc/"+allocationID+"/release", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("release status = %d body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user_id":"u1","partition":"debug","cpus":4,"memory":512,"time_limit":1}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expired salloc status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &allocation)
	expiredAllocationID := allocation["allocation_id"].(string)
	expiredCreatedAt := time.Now().Add(-2 * time.Second)
	if err := db.Model(&models.Allocation{}).Where("id = ?", expiredAllocationID).Update("created_at", expiredCreatedAt).Error; err != nil {
		t.Fatalf("move expired allocation time: %v", err)
	}
	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/srun", `{"allocation_id":"`+expiredAllocationID+`","command_line":"echo too-late"}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expired srun status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var expired models.Allocation
	if err := db.First(&expired, "id = ?", expiredAllocationID).Error; err != nil {
		t.Fatalf("load expired allocation: %v", err)
	}
	if expired.Status != models.AllocationReleased || expired.Reason != "TimeLimit" {
		t.Fatalf("expired allocation should be released with TimeLimit, got %#v", expired)
	}
	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/salloc", `{"user_id":"u1","partition":"debug","cpus":4,"memory":512}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("resources should be released after time limit, status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestSlurmScancelBulkCancelsByFilters(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	submissions := []models.Submission{
		{ID: "cancel-pending", ProblemID: "p1", UserID: "u1", Status: models.StatusQueued, Cluster: "debug", Account: "course-a", QOS: "normal"},
		{ID: "cancel-running", ProblemID: "p1", UserID: "u1", Status: models.StatusRunning, Cluster: "debug", Account: "course-a", QOS: "normal"},
		{ID: "cancel-other", ProblemID: "p1", JobName: "cancel-by-name", UserID: "u2", Status: models.StatusQueued, Cluster: "debug", Account: "course-a", QOS: "normal"},
		{ID: "cancel-array-task", ProblemID: "p1", UserID: "u2", Status: models.StatusQueued, Cluster: "debug", Account: "course-a", QOS: "normal", ArrayJobID: "array-cancel", ArrayTaskID: 3},
		{ID: "cancel-array-jobid", ProblemID: "p1", UserID: "u2", Status: models.StatusQueued, Cluster: "debug", Account: "course-a", QOS: "normal", ArrayJobID: "array-cancel", ArrayTaskID: 4},
		{ID: "cancel-array-path", ProblemID: "p1", UserID: "u2", Status: models.StatusQueued, Cluster: "debug", Account: "course-a", QOS: "normal", ArrayJobID: "array-path", ArrayTaskID: 9},
		{ID: "cancel-array-bracket-5", ProblemID: "p1", UserID: "u2", Status: models.StatusQueued, Cluster: "debug", Account: "course-a", QOS: "normal", ArrayJobID: "array-bracket", ArrayTaskID: 5},
		{ID: "cancel-array-bracket-6", ProblemID: "p1", UserID: "u2", Status: models.StatusQueued, Cluster: "debug", Account: "course-a", QOS: "normal", ArrayJobID: "array-bracket", ArrayTaskID: 6},
	}
	if err := db.Create(&submissions).Error; err != nil {
		t.Fatalf("create submissions: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scancel", "")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("empty scancel status = %d body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scancel?user_id=u0,u1&partition=other,debug&account=other,course-a&qos=debug,normal&states=PD&fields=job_id,state,cancelled", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("filtered scancel status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var page struct {
		Items     []map[string]interface{} `json:"items"`
		Matched   int                      `json:"matched"`
		Cancelled int                      `json:"cancelled"`
		Failed    int                      `json:"failed"`
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if page.Matched != 1 || page.Cancelled != 1 || page.Failed != 0 || page.Items[0]["job_id"] != "cancel-pending" || page.Items[0]["state"] != models.SlurmStateCancelled || page.Items[0]["cancelled"] != true {
		t.Fatalf("unexpected filtered scancel response: %#v", page)
	}

	var pending models.Submission
	if err := db.Where("id = ?", "cancel-pending").First(&pending).Error; err != nil {
		t.Fatalf("load cancelled pending job: %v", err)
	}
	if pending.Status != models.StatusFailed || pending.Reason != "Interrupted" {
		t.Fatalf("unexpected pending cancellation state: %#v", pending)
	}
	var running models.Submission
	if err := db.Where("id = ?", "cancel-running").First(&running).Error; err != nil {
		t.Fatalf("load running job: %v", err)
	}
	if running.Status != models.StatusRunning {
		t.Fatalf("running job should not be cancelled by pending filter: %#v", running)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scancel", `{"job_ids":["cancel-running"]}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("job_ids scancel status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if page.Matched != 1 || page.Cancelled != 1 || page.Items[0]["job_id"] != "cancel-running" {
		t.Fatalf("unexpected job_ids scancel response: %#v", page)
	}
	if err := db.Where("id = ?", "cancel-running").First(&running).Error; err != nil {
		t.Fatalf("reload running job: %v", err)
	}
	if running.Status != models.StatusFailed || running.Reason != "Interrupted" {
		t.Fatalf("unexpected running cancellation state: %#v", running)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scancel?job_name=cancel-by-name&fields=job_id,job_name,problem_id,cancelled", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("job_name scancel status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if page.Matched != 1 || page.Cancelled != 1 || page.Items[0]["job_id"] != "cancel-other" || page.Items[0]["job_name"] != "cancel-by-name" || page.Items[0]["problem_id"] != "p1" {
		t.Fatalf("unexpected job_name scancel response: %#v", page)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scancel?array_job_id=array-cancel&array_task_id=3&fields=job_id,array_job_id,array_task_id,cancelled", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("array scancel status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if page.Matched != 1 || page.Cancelled != 1 || page.Items[0]["job_id"] != "cancel-array-task" || page.Items[0]["array_job_id"] != "array-cancel" || page.Items[0]["array_task_id"].(float64) != 3 {
		t.Fatalf("unexpected array scancel response: %#v", page)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scancel?job_id=array-cancel_4&fields=job_id,array_job_id,array_task_id,cancelled", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("array job-id scancel status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if page.Matched != 1 || page.Cancelled != 1 || page.Items[0]["job_id"] != "cancel-array-jobid" || page.Items[0]["array_job_id"] != "array-cancel" || page.Items[0]["array_task_id"].(float64) != 4 {
		t.Fatalf("unexpected array job-id scancel response: %#v", page)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scancel?job_id=array-bracket_%5B5,6%5D&fields=job_id,array_job_id,array_task_id,cancelled", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("bracket array job-id scancel status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	seenCancelledTasks := make(map[float64]bool)
	for _, item := range page.Items {
		if item["array_job_id"] != "array-bracket" || item["cancelled"] != true {
			t.Fatalf("unexpected bracket array scancel item: %#v", item)
		}
		seenCancelledTasks[item["array_task_id"].(float64)] = true
	}
	if page.Matched != 2 || page.Cancelled != 2 || !seenCancelledTasks[5] || !seenCancelledTasks[6] {
		t.Fatalf("unexpected bracket array job-id scancel response: %#v", page)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scancel/array-path_9", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("array path scancel status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var pathCancelled models.Submission
	if err := db.Where("id = ?", "cancel-array-path").First(&pathCancelled).Error; err != nil {
		t.Fatalf("load array path cancelled job: %v", err)
	}
	if pathCancelled.Status != models.StatusFailed || pathCancelled.Reason != "Interrupted" {
		t.Fatalf("unexpected array path cancellation state: %#v", pathCancelled)
	}

	var events int64
	if err := db.Model(&models.AccountingRecord{}).Where("submission_id IN ? AND event = ?", []string{"cancel-pending", "cancel-running", "cancel-other", "cancel-array-task", "cancel-array-jobid", "cancel-array-path", "cancel-array-bracket-5", "cancel-array-bracket-6"}, database.AccountEventInterrupted).Count(&events).Error; err != nil {
		t.Fatalf("count scancel accounting events: %v", err)
	}
	if events != 8 {
		t.Fatalf("scancel accounting events = %d, want 8", events)
	}
}

func TestSlurmScontrolArrayWideJobActions(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	submissions := []models.Submission{
		{ID: "array-hold", ProblemID: "p1", UserID: "u1", Status: models.StatusQueued, Cluster: "debug", ArrayJobID: "array-hold", ArrayTaskID: 1},
		{ID: "array-hold-2", ProblemID: "p1", UserID: "u1", Status: models.StatusQueued, Cluster: "debug", ArrayJobID: "array-hold", ArrayTaskID: 2},
		{ID: "array-requeue", ProblemID: "p1", UserID: "u1", Status: models.StatusFailed, Cluster: "debug", Node: "n1", AllocatedCores: "0", AllocatedNodeCores: "n1:0", ArrayJobID: "array-requeue", ArrayTaskID: 1, Score: 10, Performance: 1, Info: models.JSONMap{"old": true}},
		{ID: "array-requeue-2", ProblemID: "p1", UserID: "u1", Status: models.StatusSuccess, Cluster: "debug", Node: "n1", AllocatedCores: "1", AllocatedNodeCores: "n1:1", ArrayJobID: "array-requeue", ArrayTaskID: 2, Score: 20, Performance: 2, Info: models.JSONMap{"old": true}},
		{ID: "array-cancel-wide", ProblemID: "p1", UserID: "u1", Status: models.StatusQueued, Cluster: "debug", ArrayJobID: "array-cancel-wide", ArrayTaskID: 1},
		{ID: "array-cancel-wide-2", ProblemID: "p1", UserID: "u1", Status: models.StatusQueued, Cluster: "debug", ArrayJobID: "array-cancel-wide", ArrayTaskID: 2},
	}
	if err := db.Create(&submissions).Error; err != nil {
		t.Fatalf("create array action submissions: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scontrol/hold/array-hold?fields=job_id,array_task_id,held", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("array hold status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var page struct {
		Items     []map[string]interface{} `json:"items"`
		Matched   int                      `json:"matched"`
		Held      int                      `json:"held"`
		Released  int                      `json:"released"`
		Requeued  int                      `json:"requeued"`
		Cancelled int                      `json:"cancelled"`
		Failed    int                      `json:"failed"`
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if page.Matched != 2 || page.Held != 2 || page.Failed != 0 {
		t.Fatalf("unexpected array hold response: %#v", page)
	}
	var heldCount int64
	if err := db.Model(&models.Submission{}).Where("array_job_id = ? AND hold = ?", "array-hold", true).Count(&heldCount).Error; err != nil {
		t.Fatalf("count held array tasks: %v", err)
	}
	if heldCount != 2 {
		t.Fatalf("held array tasks = %d, want 2", heldCount)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scontrol/release/array-hold_2?fields=job_id,array_task_id,released", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("array task release status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if page.Matched != 1 || page.Released != 1 || page.Items[0]["array_task_id"].(float64) != 2 {
		t.Fatalf("unexpected array task release response: %#v", page)
	}
	var task1, task2 models.Submission
	if err := db.First(&task1, "id = ?", "array-hold").Error; err != nil {
		t.Fatalf("load task1: %v", err)
	}
	if err := db.First(&task2, "id = ?", "array-hold-2").Error; err != nil {
		t.Fatalf("load task2: %v", err)
	}
	if !task1.Hold || task2.Hold {
		t.Fatalf("release should only affect selected task: task1=%#v task2=%#v", task1, task2)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scontrol/requeue/array-requeue?fields=job_id,array_task_id,requeued", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("array requeue status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if page.Matched != 2 || page.Requeued != 2 || page.Failed != 0 {
		t.Fatalf("unexpected array requeue response: %#v", page)
	}
	var requeued []models.Submission
	if err := db.Order("array_task_id asc").Find(&requeued, "array_job_id = ?", "array-requeue").Error; err != nil {
		t.Fatalf("load requeued array: %v", err)
	}
	if len(requeued) != 2 || requeued[0].Status != models.StatusQueued || requeued[1].Status != models.StatusQueued || requeued[0].Node != "" || requeued[0].AllocatedNodeCores != "" || requeued[0].Score != 0 {
		t.Fatalf("unexpected requeued array state: %#v", requeued)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scontrol/cancel/array-cancel-wide?fields=job_id,array_task_id,cancelled,state", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("array cancel status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if page.Matched != 2 || page.Cancelled != 2 || page.Failed != 0 {
		t.Fatalf("unexpected array cancel response: %#v", page)
	}
	var cancelledCount int64
	if err := db.Model(&models.Submission{}).Where("array_job_id = ? AND status = ? AND reason = ?", "array-cancel-wide", models.StatusFailed, "Interrupted").Count(&cancelledCount).Error; err != nil {
		t.Fatalf("count cancelled array tasks: %v", err)
	}
	if cancelledCount != 2 {
		t.Fatalf("cancelled array tasks = %d, want 2", cancelledCount)
	}

	var events int64
	if err := db.Model(&models.AccountingRecord{}).Where("submission_id IN ? AND event IN ?", []string{"array-hold", "array-hold-2", "array-requeue", "array-requeue-2", "array-cancel-wide", "array-cancel-wide-2"}, []string{database.AccountEventHeld, database.AccountEventReleased, database.AccountEventRequeued, database.AccountEventInterrupted}).Count(&events).Error; err != nil {
		t.Fatalf("count array action accounting events: %v", err)
	}
	if events != 7 {
		t.Fatalf("array action accounting events = %d, want 7", events)
	}
}

func TestSlurmScancelBulkSignalsWithoutCancelling(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	if err := db.Create(&models.Submission{
		ID:        "signal-running",
		ProblemID: "p1",
		UserID:    "u1",
		Status:    models.StatusRunning,
		Cluster:   "debug",
		Node:      "n1",
		Account:   "course-a",
		QOS:       "normal",
	}).Error; err != nil {
		t.Fatalf("create running submission: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scancel?job_id=signal-running&signal=USR1&fields=job_id,state,signaled,signal,cancelled", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("scancel signal status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var page struct {
		Items     []map[string]interface{} `json:"items"`
		Matched   int                      `json:"matched"`
		Cancelled int                      `json:"cancelled"`
		Signaled  int                      `json:"signaled"`
		Failed    int                      `json:"failed"`
	}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &page)
	if page.Matched != 1 || page.Signaled != 1 || page.Cancelled != 0 || page.Failed != 0 {
		t.Fatalf("unexpected scancel signal page: %#v", page)
	}
	if len(page.Items) != 1 || page.Items[0]["job_id"] != "signal-running" || page.Items[0]["state"] != models.SlurmStateRunning || page.Items[0]["signaled"] != true || page.Items[0]["signal"] != "SIGUSR1" || page.Items[0]["cancelled"] != false {
		t.Fatalf("unexpected scancel signal item: %#v", page.Items)
	}

	var sub models.Submission
	if err := db.Where("id = ?", "signal-running").First(&sub).Error; err != nil {
		t.Fatalf("load signaled job: %v", err)
	}
	if sub.Status != models.StatusRunning || sub.Reason != "" {
		t.Fatalf("signal should not cancel running job: %#v", sub)
	}

	var signaledEvents int64
	if err := db.Model(&models.AccountingRecord{}).Where("submission_id = ? AND event = ? AND message = ?", sub.ID, database.AccountEventSignaled, "SIGUSR1").Count(&signaledEvents).Error; err != nil {
		t.Fatalf("count signaled accounting events: %v", err)
	}
	if signaledEvents != 1 {
		t.Fatalf("signaled accounting events = %d, want 1", signaledEvents)
	}
	var interruptedEvents int64
	if err := db.Model(&models.AccountingRecord{}).Where("submission_id = ? AND event = ?", sub.ID, database.AccountEventInterrupted).Count(&interruptedEvents).Error; err != nil {
		t.Fatalf("count interrupted accounting events: %v", err)
	}
	if interruptedEvents != 0 {
		t.Fatalf("interrupted accounting events = %d, want 0", interruptedEvents)
	}
}

func TestSlurmSuspendAndResumeJob(t *testing.T) {
	router, db := newSlurmTestRouter(t)
	if err := db.Create(&models.Submission{
		ID:        "job-4",
		ProblemID: "p1",
		UserID:    "u1",
		Status:    models.StatusRunning,
		Cluster:   "debug",
		Node:      "n1",
	}).Error; err != nil {
		t.Fatalf("create running submission: %v", err)
	}
	if err := db.Create(&models.Container{
		ID:           "container-4",
		SubmissionID: "job-4",
		UserID:       "u1",
		Status:       models.StatusRunning,
	}).Error; err != nil {
		t.Fatalf("create running container: %v", err)
	}

	recorder := performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scontrol/suspend/job-4", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("suspend status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var sub models.Submission
	if err := db.Where("id = ?", "job-4").First(&sub).Error; err != nil {
		t.Fatalf("load suspended submission: %v", err)
	}
	if sub.Status != models.StatusSuspended || sub.Reason != "Suspended" {
		t.Fatalf("unexpected suspended submission: %#v", sub)
	}
	var container models.Container
	if err := db.Where("id = ?", "container-4").First(&container).Error; err != nil {
		t.Fatalf("load suspended container: %v", err)
	}
	if container.Status != models.StatusSuspended {
		t.Fatalf("container status = %s, want Suspended", container.Status)
	}

	recorder = performSlurmRequest(router, http.MethodGet, "/api/v1/slurm/squeue?job_id=job-4&fields=job_id,state,reason", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("squeue status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var items []map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &items)
	if len(items) != 1 || items[0]["state"] != models.SlurmStateSuspended {
		t.Fatalf("unexpected squeue suspended item: %#v", items)
	}

	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scontrol/resume/job-4", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("resume status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if err := db.Where("id = ?", "job-4").First(&sub).Error; err != nil {
		t.Fatalf("load resumed submission: %v", err)
	}
	if sub.Status != models.StatusRunning || sub.Reason != "" {
		t.Fatalf("unexpected resumed submission: %#v", sub)
	}
	recorder = performSlurmRequest(router, http.MethodPost, "/api/v1/slurm/scontrol/signal/job-4?fields=job_id,state,signal", `{"signal":"USR1"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("signal status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var signaled map[string]interface{}
	decodeSlurmResponseData(t, recorder.Body.Bytes(), &signaled)
	if signaled["job_id"] != "job-4" || signaled["state"] != models.SlurmStateRunning || signaled["signal"] != "SIGUSR1" {
		t.Fatalf("unexpected signal response: %#v", signaled)
	}
	var events int64
	if err := db.Model(&models.AccountingRecord{}).Where("submission_id = ? AND event IN ?", "job-4", []string{database.AccountEventSuspended, database.AccountEventResumed, database.AccountEventSignaled}).Count(&events).Error; err != nil {
		t.Fatalf("count suspend/resume accounting: %v", err)
	}
	if events != 3 {
		t.Fatalf("accounting event count = %d, want 3", events)
	}
}

func newSlurmTestRouter(t *testing.T) (*gin.Engine, *gorm.DB) {
	router, db, _ := newSlurmTestRouterWithScheduler(t)
	return router, db
}

func newSlurmTestRouterWithScheduler(t *testing.T) (*gin.Engine, *gorm.DB, *judger.Scheduler) {
	t.Helper()
	return newSlurmTestRouterWithNodesAndScheduler(t, []config.Node{{
		Name:   "n1",
		CPU:    4,
		Memory: 4096,
	}})
}

func newSlurmTestRouterWithNodes(t *testing.T, nodes []config.Node) (*gin.Engine, *gorm.DB) {
	router, db, _ := newSlurmTestRouterWithNodesAndScheduler(t, nodes)
	return router, db
}

func newSlurmTestRouterWithNodesAndScheduler(t *testing.T, nodes []config.Node) (*gin.Engine, *gorm.DB, *judger.Scheduler) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{
		Cluster: []config.Cluster{{
			Name:         "debug",
			PriorityTier: 2,
			Nodes:        nodes,
		}},
		Scheduler: config.Scheduler{
			Licenses: map[string]int{"license/foo": 2},
			PriorityWeights: config.PriorityWeights{
				Age:       1,
				QOS:       10,
				Nice:      2,
				Partition: 100,
				JobSize:   3,
				Fairshare: 5,
			},
			FairshareDecay: config.FairshareDecay{
				Enabled:       true,
				HalfLifeHours: 24,
				UsageWeight:   1,
			},
			QOS: []config.QOS{
				{Name: "normal", Priority: 4},
			},
			Accounts: []config.Account{
				{Name: "course-a", Users: []string{"u1", "alice"}, AllowQOS: []string{"normal"}, Fairshare: 20},
				{Name: "course-b", Fairshare: 10},
			},
		},
		Storage: config.Storage{
			SubmissionContent: t.TempDir(),
			SubmissionLog:     t.TempDir(),
		},
	}
	db, err := database.Init("file:" + strings.ReplaceAll(t.Name(), "/", "_") + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("init db: %v", err)
	}
	if err := db.Create(&models.User{ID: "u1", Username: "alice"}).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	appState := &judger.AppState{
		Contests: make(map[string]*judger.Contest),
		Problems: map[string]*judger.Problem{
			"p1": {ID: "p1", Cluster: "debug", CPU: 1, Memory: 128},
		},
		ProblemToContestMap: make(map[string]*judger.Contest),
	}
	scheduler := judger.NewScheduler(cfg, db, appState)
	return NewAdminRouter(cfg, db, scheduler, appState), db, scheduler
}

func performSlurmRequest(router *gin.Engine, method, path string, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	return recorder
}

func decodeSlurmResponseData(t *testing.T, body []byte, target interface{}) {
	t.Helper()
	var response struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode response: %v body=%s", err, string(body))
	}
	if err := json.Unmarshal(response.Data, target); err != nil {
		t.Fatalf("decode response data: %v body=%s", err, string(response.Data))
	}
}

type fakeRuntime struct {
	stdout         string
	stderr         string
	exitCode       int
	createdVolumes []string
	removedVolumes []string
	createdNames   []string
	createdCPUs    []int
	createdMemory  []int64
	createdCpuset  []string
	createdEnvs    [][]string
	startedIDs     []string
	cleanedIDs     []string
	signaledIDs    []string
	signals        []string
	execCommands   [][]string
	execDeadlines  []time.Duration
	copiedSrcDirs  []string
	copiedDstDirs  []string
	usage          judger.RuntimeUsage
}

func (f *fakeRuntime) CreateVolume(name string) error {
	f.createdVolumes = append(f.createdVolumes, name)
	return nil
}

func (f *fakeRuntime) RemoveVolume(name string) error {
	f.removedVolumes = append(f.removedVolumes, name)
	return nil
}

func (f *fakeRuntime) CreateContainer(image, volumeName string, cpu int, cpusetCpus string, memory int64, asRoot bool, customMounts []judger.Mount, networkEnabled bool, name string, envs []string) (string, error) {
	f.createdNames = append(f.createdNames, name)
	f.createdCPUs = append(f.createdCPUs, cpu)
	f.createdMemory = append(f.createdMemory, memory)
	f.createdCpuset = append(f.createdCpuset, cpusetCpus)
	f.createdEnvs = append(f.createdEnvs, append([]string(nil), envs...))
	return "runtime-" + name, nil
}

func (f *fakeRuntime) StartContainer(containerID string) error {
	f.startedIDs = append(f.startedIDs, containerID)
	return nil
}

func (f *fakeRuntime) ExecInContainer(ctx context.Context, containerID string, cmd []string, outputCallback func(streamType string, data []byte)) (judger.ExecResult, error) {
	f.execCommands = append(f.execCommands, append([]string(nil), cmd...))
	if deadline, ok := ctx.Deadline(); ok {
		f.execDeadlines = append(f.execDeadlines, time.Until(deadline))
	}
	if f.stdout != "" {
		outputCallback("stdout", []byte(f.stdout))
	}
	if f.stderr != "" {
		outputCallback("stderr", []byte(f.stderr))
	}
	return judger.ExecResult{Stdout: f.stdout, Stderr: f.stderr, ExitCode: f.exitCode, Usage: f.usage}, nil
}

func (f *fakeRuntime) PauseContainer(containerID string) error {
	return nil
}

func (f *fakeRuntime) ResumeContainer(containerID string) error {
	return nil
}

func (f *fakeRuntime) SignalContainer(containerID string, signal string) error {
	f.signaledIDs = append(f.signaledIDs, containerID)
	f.signals = append(f.signals, signal)
	return nil
}

func (f *fakeRuntime) CleanupContainer(containerID string) {
	f.cleanedIDs = append(f.cleanedIDs, containerID)
}

func (f *fakeRuntime) CopyToContainer(containerID string, srcDir string, dstDir string) error {
	f.copiedSrcDirs = append(f.copiedSrcDirs, srcDir)
	f.copiedDstDirs = append(f.copiedDstDirs, dstDir)
	return nil
}
