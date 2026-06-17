package admin

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"github.com/ZJUSCT/CSOJ/internal/judger"
	"github.com/ZJUSCT/CSOJ/internal/util"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type slurmBatchRequest struct {
	UserID       string            `json:"user_id"`
	User         string            `json:"user"`
	ProblemID    string            `json:"problem_id"`
	JobName      string            `json:"job_name"`
	Name         string            `json:"name"`
	WorkDir      string            `json:"work_dir"`
	Chdir        string            `json:"chdir"`
	StdinPath    string            `json:"stdin_path"`
	Input        string            `json:"input"`
	StdoutPath   string            `json:"stdout_path"`
	Output       string            `json:"output"`
	StderrPath   string            `json:"stderr_path"`
	ErrorPath    string            `json:"error"`
	OpenMode     string            `json:"open_mode"`
	Comment      string            `json:"comment"`
	MailType     string            `json:"mail_type"`
	MailUser     string            `json:"mail_user"`
	Exclusive    *bool             `json:"exclusive"`
	Requeue      *bool             `json:"requeue"`
	Export       string            `json:"export"`
	Environment  models.JSONMap    `json:"environment"`
	Partition    string            `json:"partition"`
	Cluster      string            `json:"cluster"`
	Account      string            `json:"account"`
	QOS          string            `json:"qos"`
	Priority     *int              `json:"priority"`
	Nice         *int              `json:"nice"`
	Hold         *bool             `json:"hold"`
	CPU          *int              `json:"cpus"`
	NTasks       *int              `json:"ntasks"`
	Tasks        *int              `json:"tasks"`
	CPUsPerTask  *int              `json:"cpus_per_task"`
	Nodes        *int              `json:"nodes"`
	Memory       *slurmMemoryMB    `json:"memory"`
	Mem          *slurmMemoryMB    `json:"mem"`
	MemoryPerCPU *slurmMemoryMB    `json:"mem_per_cpu"`
	MemPerCPU    *slurmMemoryMB    `json:"memory_per_cpu"`
	BeginTime    *slurmDateTime    `json:"begin_time"`
	Begin        *slurmDateTime    `json:"begin"`
	StartTime    *slurmDateTime    `json:"start_time"`
	Deadline     *slurmDateTime    `json:"deadline"`
	TimeLimit    *slurmTimeLimit   `json:"time_limit"`
	Time         *slurmTimeLimit   `json:"time"`
	Dependencies string            `json:"dependencies"`
	Reservation  string            `json:"reservation"`
	NodeList     string            `json:"node_list"`
	Nodelist     string            `json:"nodelist"`
	NodesList    string            `json:"nodeslist"`
	Exclude      string            `json:"exclude"`
	ExcludeNodes string            `json:"exclude_nodes"`
	Constraint   string            `json:"constraint"`
	GRES         string            `json:"gres"`
	TRES         string            `json:"tres"`
	Licenses     string            `json:"licenses"`
	Array        string            `json:"array"`
	Files        map[string]string `json:"files"`
	FilesBase64  map[string]string `json:"files_base64"`
	Script       string            `json:"script"`
	ScriptPath   string            `json:"script_path"`
	Wrap         string            `json:"wrap"`
}

type slurmTimeLimit int

func (limit slurmTimeLimit) Int() int {
	return int(limit)
}

func (limit *slurmTimeLimit) UnmarshalJSON(data []byte) error {
	value := strings.TrimSpace(string(data))
	if value == "" || value == "null" {
		return nil
	}
	if strings.HasPrefix(value, `"`) {
		unquoted, err := strconv.Unquote(value)
		if err != nil {
			return fmt.Errorf("invalid time limit string: %w", err)
		}
		seconds, err := parseSlurmTimeLimit(unquoted)
		if err != nil {
			return err
		}
		*limit = slurmTimeLimit(seconds)
		return nil
	}
	seconds, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("invalid time limit seconds %q", value)
	}
	if seconds < 0 {
		return fmt.Errorf("time limit seconds must be non-negative")
	}
	*limit = slurmTimeLimit(seconds)
	return nil
}

type slurmMemoryMB int64

func (memory slurmMemoryMB) Int64() int64 {
	return int64(memory)
}

func (memory *slurmMemoryMB) UnmarshalJSON(data []byte) error {
	value := strings.TrimSpace(string(data))
	if value == "" || value == "null" {
		return nil
	}
	if strings.HasPrefix(value, `"`) {
		unquoted, err := strconv.Unquote(value)
		if err != nil {
			return fmt.Errorf("invalid memory string: %w", err)
		}
		mb, err := parseSlurmMemoryMB(unquoted)
		if err != nil {
			return err
		}
		*memory = slurmMemoryMB(mb)
		return nil
	}
	mb, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid memory MB %q", value)
	}
	if mb < 0 {
		return fmt.Errorf("memory MB must be non-negative")
	}
	*memory = slurmMemoryMB(mb)
	return nil
}

type slurmDateTime struct {
	time.Time
}

func (dt slurmDateTime) Ptr() *time.Time {
	value := dt.Time
	return &value
}

func (dt *slurmDateTime) UnmarshalJSON(data []byte) error {
	value := strings.TrimSpace(string(data))
	if value == "" || value == "null" {
		return nil
	}
	if !strings.HasPrefix(value, `"`) {
		return fmt.Errorf("datetime must be a string")
	}
	unquoted, err := strconv.Unquote(value)
	if err != nil {
		return fmt.Errorf("invalid datetime string: %w", err)
	}
	parsed, err := parseSlurmDirectiveTime(unquoted)
	if err != nil {
		return err
	}
	dt.Time = parsed
	return nil
}

func firstPositiveMemory(values ...slurmMemoryMB) int64 {
	for _, value := range values {
		if value > 0 {
			return value.Int64()
		}
	}
	return 0
}

func firstSlurmDateTime(values ...*slurmDateTime) *slurmDateTime {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

type slurmStringList []string

func (list *slurmStringList) UnmarshalJSON(data []byte) error {
	value := strings.TrimSpace(string(data))
	if value == "" || value == "null" {
		return nil
	}
	if strings.HasPrefix(value, `"`) {
		unquoted, err := strconv.Unquote(value)
		if err != nil {
			return fmt.Errorf("invalid string list value: %w", err)
		}
		*list = slurmStringList(slurmCSVValues(unquoted))
		return nil
	}
	var values []string
	if err := json.Unmarshal(data, &values); err == nil {
		*list = slurmStringList(slurmCSVValues(values...))
		return nil
	}
	var rawValues []interface{}
	if err := json.Unmarshal(data, &rawValues); err != nil {
		return fmt.Errorf("string list must be a string or array")
	}
	values = make([]string, 0, len(rawValues))
	for _, rawValue := range rawValues {
		values = append(values, fmt.Sprint(rawValue))
	}
	*list = slurmStringList(slurmCSVValues(values...))
	return nil
}

func (list slurmStringList) Strings() []string {
	if len(list) == 0 {
		return nil
	}
	return slurmCSVValues([]string(list)...)
}

func mergeSlurmStringLists(lists ...slurmStringList) []string {
	values := make([]string, 0)
	for _, list := range lists {
		values = append(values, list.Strings()...)
	}
	return slurmCSVValues(values...)
}

type slurmAccountRequest struct {
	Name              string          `json:"name"`
	Account           string          `json:"account"`
	Users             slurmStringList `json:"users"`
	User              slurmStringList `json:"user"`
	AllowQOS          slurmStringList `json:"allow_qos"`
	AllowedQOS        slurmStringList `json:"allowed_qos"`
	QOS               slurmStringList `json:"qos"`
	MaxJobs           int             `json:"max_jobs"`
	MaxSubmit         int             `json:"max_submit"`
	MaxBillingRunning float64         `json:"max_billing_running"`
	MaxBillingSubmit  float64         `json:"max_billing_submit"`
	Fairshare         int             `json:"fairshare"`
	Parent            string          `json:"parent"`
	ParentAccount     string          `json:"parent_account"`
}

func (req slurmAccountRequest) Config(pathName string) config.Account {
	return config.Account{
		Name:              firstNonEmpty(pathName, req.Name, req.Account),
		Users:             mergeSlurmStringLists(req.Users, req.User),
		AllowQOS:          mergeSlurmStringLists(req.AllowQOS, req.AllowedQOS, req.QOS),
		MaxJobs:           req.MaxJobs,
		MaxSubmit:         req.MaxSubmit,
		MaxBillingRunning: req.MaxBillingRunning,
		MaxBillingSubmit:  req.MaxBillingSubmit,
		Fairshare:         req.Fairshare,
		ParentName:        firstNonEmpty(req.Parent, req.ParentAccount),
	}
}

type slurmUserRequest struct {
	Name              string          `json:"name"`
	User              string          `json:"user"`
	UserID            string          `json:"user_id"`
	Account           string          `json:"account"`
	DefaultAccount    string          `json:"default_account"`
	QOS               slurmStringList `json:"qos"`
	AllowQOS          slurmStringList `json:"allow_qos"`
	AllowedQOS        slurmStringList `json:"allowed_qos"`
	Fairshare         *int            `json:"fairshare"`
	MaxJobs           *int            `json:"max_jobs"`
	MaxSubmit         *int            `json:"max_submit"`
	MaxBillingRunning *float64        `json:"max_billing_running"`
	MaxBillingSubmit  *float64        `json:"max_billing_submit"`
}

type slurmQOSRequest struct {
	Name                     string          `json:"name"`
	QOS                      string          `json:"qos"`
	Priority                 int             `json:"priority"`
	MaxJobsPerUser           int             `json:"max_jobs_per_user"`
	MaxJobs                  int             `json:"max_jobs"`
	MaxSubmitJobsPerUser     int             `json:"max_submit_jobs_per_user"`
	MaxSubmit                int             `json:"max_submit"`
	MaxCPUPerJob             int             `json:"max_cpu_per_job"`
	MaxCPUsPerJob            int             `json:"max_cpus_per_job"`
	MaxMemoryPerJob          slurmMemoryMB   `json:"max_memory_per_job"`
	MaxMemPerJob             slurmMemoryMB   `json:"max_mem_per_job"`
	MaxBillingPerJob         float64         `json:"max_billing_per_job"`
	MaxBillingPerUserRunning float64         `json:"max_billing_per_user_running"`
	MaxBillingRunning        float64         `json:"max_billing_running"`
	MaxBillingPerUserSubmit  float64         `json:"max_billing_per_user_submit"`
	MaxBillingSubmit         float64         `json:"max_billing_submit"`
	MaxTime                  *slurmTimeLimit `json:"max_time"`
	TimeLimit                *slurmTimeLimit `json:"time_limit"`
	Preempt                  slurmStringList `json:"preempt"`
	PreemptQOS               slurmStringList `json:"preempt_qos"`
}

func (req slurmQOSRequest) Config(pathName string) config.QOS {
	maxJobs := req.MaxJobsPerUser
	if maxJobs == 0 {
		maxJobs = req.MaxJobs
	}
	maxSubmit := req.MaxSubmitJobsPerUser
	if maxSubmit == 0 {
		maxSubmit = req.MaxSubmit
	}
	maxCPU := req.MaxCPUPerJob
	if maxCPU == 0 {
		maxCPU = req.MaxCPUsPerJob
	}
	maxBillingRunning := req.MaxBillingPerUserRunning
	if maxBillingRunning == 0 {
		maxBillingRunning = req.MaxBillingRunning
	}
	maxBillingSubmit := req.MaxBillingPerUserSubmit
	if maxBillingSubmit == 0 {
		maxBillingSubmit = req.MaxBillingSubmit
	}
	maxTime := 0
	if req.MaxTime != nil {
		maxTime = req.MaxTime.Int()
	} else if req.TimeLimit != nil {
		maxTime = req.TimeLimit.Int()
	}
	return config.QOS{
		Name:                     firstNonEmpty(pathName, req.Name, req.QOS),
		Priority:                 req.Priority,
		MaxJobsPerUser:           maxJobs,
		MaxSubmitJobsPerUser:     maxSubmit,
		MaxCPUPerJob:             maxCPU,
		MaxMemoryPerJob:          firstPositiveMemory(req.MaxMemoryPerJob, req.MaxMemPerJob),
		MaxBillingPerJob:         req.MaxBillingPerJob,
		MaxBillingPerUserRunning: maxBillingRunning,
		MaxBillingPerUserSubmit:  maxBillingSubmit,
		MaxTime:                  maxTime,
		Preempt:                  mergeSlurmStringLists(req.Preempt, req.PreemptQOS),
	}
}

type slurmPartitionRequest struct {
	Name          string          `json:"name"`
	Partition     string          `json:"partition"`
	State         string          `json:"state"`
	PriorityTier  int             `json:"priority_tier"`
	Priority      int             `json:"priority"`
	MaxTime       *slurmTimeLimit `json:"max_time"`
	TimeLimit     *slurmTimeLimit `json:"time_limit"`
	MaxJobs       int             `json:"max_jobs"`
	AllowUsers    slurmStringList `json:"allow_users"`
	Users         slurmStringList `json:"users"`
	AllowAccounts slurmStringList `json:"allow_accounts"`
	Accounts      slurmStringList `json:"accounts"`
	AllowQOS      slurmStringList `json:"allow_qos"`
	AllowedQOS    slurmStringList `json:"allowed_qos"`
	DenyQOS       slurmStringList `json:"deny_qos"`
}

func (req slurmPartitionRequest) Config(pathName string) config.Cluster {
	priority := req.PriorityTier
	if priority == 0 {
		priority = req.Priority
	}
	maxTime := 0
	if req.MaxTime != nil {
		maxTime = req.MaxTime.Int()
	} else if req.TimeLimit != nil {
		maxTime = req.TimeLimit.Int()
	}
	return config.Cluster{
		Name:          firstNonEmpty(pathName, req.Name, req.Partition),
		State:         normalizeSlurmPartitionState(req.State),
		PriorityTier:  priority,
		MaxTime:       maxTime,
		MaxJobs:       req.MaxJobs,
		AllowUsers:    mergeSlurmStringLists(req.AllowUsers, req.Users),
		AllowAccounts: mergeSlurmStringLists(req.AllowAccounts, req.Accounts),
		AllowQOS:      mergeSlurmStringLists(req.AllowQOS, req.AllowedQOS),
		DenyQOS:       mergeSlurmStringLists(req.DenyQOS),
	}
}

type slurmAllocationRequest struct {
	UserID       string          `json:"user_id"`
	User         string          `json:"user"`
	Partition    string          `json:"partition"`
	Cluster      string          `json:"cluster"`
	CPU          int             `json:"cpus"`
	Memory       slurmMemoryMB   `json:"memory"`
	Mem          slurmMemoryMB   `json:"mem"`
	Nodes        int             `json:"nodes"`
	Account      string          `json:"account"`
	QOS          string          `json:"qos"`
	TRES         string          `json:"tres"`
	GRES         string          `json:"gres"`
	TimeLimit    *slurmTimeLimit `json:"time_limit"`
	Time         *slurmTimeLimit `json:"time"`
	Constraint   string          `json:"constraint"`
	Reservation  string          `json:"reservation"`
	NodeList     string          `json:"node_list"`
	Nodelist     string          `json:"nodelist"`
	NodesList    string          `json:"nodeslist"`
	Exclude      string          `json:"exclude"`
	ExcludeNodes string          `json:"exclude_nodes"`
	Exclusive    bool            `json:"exclusive"`
}

func (req slurmAllocationRequest) InteractiveRequest() judger.InteractiveAllocationRequest {
	timeLimit := 0
	if req.TimeLimit != nil {
		timeLimit = req.TimeLimit.Int()
	} else if req.Time != nil {
		timeLimit = req.Time.Int()
	}
	return judger.InteractiveAllocationRequest{
		UserID:       firstNonEmpty(req.UserID, req.User),
		Cluster:      firstNonEmpty(req.Partition, req.Cluster),
		CPU:          req.CPU,
		Memory:       firstPositiveMemory(req.Memory, req.Mem),
		Nodes:        req.Nodes,
		Account:      req.Account,
		QOS:          req.QOS,
		TRES:         req.TRES,
		GRES:         req.GRES,
		TimeLimit:    timeLimit,
		Constraint:   req.Constraint,
		Reservation:  req.Reservation,
		NodeList:     firstNonEmpty(req.NodeList, req.Nodelist, req.NodesList),
		ExcludeNodes: firstNonEmpty(req.ExcludeNodes, req.Exclude),
		Exclusive:    req.Exclusive,
	}
}

type slurmReservationRequest struct {
	Name          string          `json:"name"`
	Reservation   string          `json:"reservation"`
	Cluster       string          `json:"cluster"`
	Partition     string          `json:"partition"`
	Nodes         slurmStringList `json:"nodes"`
	NodeList      slurmStringList `json:"node_list"`
	Nodelist      slurmStringList `json:"nodelist"`
	NodesList     slurmStringList `json:"nodeslist"`
	Users         slurmStringList `json:"users"`
	User          slurmStringList `json:"user"`
	Accounts      slurmStringList `json:"accounts"`
	Account       slurmStringList `json:"account"`
	Starttime     *slurmDateTime  `json:"starttime"`
	StartTime     *slurmDateTime  `json:"start_time"`
	Start         *slurmDateTime  `json:"start"`
	Endtime       *slurmDateTime  `json:"endtime"`
	EndTime       *slurmDateTime  `json:"end_time"`
	End           *slurmDateTime  `json:"end"`
	Duration      *slurmTimeLimit `json:"duration"`
	CPU           int             `json:"cpu"`
	Memory        slurmMemoryMB   `json:"memory"`
	Mem           slurmMemoryMB   `json:"mem"`
	AllowOverlap  bool            `json:"allow_overlap"`
	IgnoreRunning bool            `json:"ignore_running"`
}

func (req slurmReservationRequest) Config(pathName string) config.Reservation {
	reservation := config.Reservation{
		Name:          firstNonEmpty(pathName, req.Name, req.Reservation),
		Cluster:       firstNonEmpty(req.Cluster, req.Partition),
		Nodes:         mergeSlurmStringLists(req.Nodes, req.NodeList, req.Nodelist, req.NodesList),
		Users:         mergeSlurmStringLists(req.Users, req.User),
		Accounts:      mergeSlurmStringLists(req.Accounts, req.Account),
		CPU:           req.CPU,
		Memory:        firstPositiveMemory(req.Memory, req.Mem),
		AllowOverlap:  req.AllowOverlap,
		IgnoreRunning: req.IgnoreRunning,
	}
	if start := firstSlurmDateTime(req.Starttime, req.StartTime, req.Start); start != nil {
		reservation.StartTime = start.Time
	}
	if end := firstSlurmDateTime(req.Endtime, req.EndTime, req.End); end != nil {
		reservation.EndTime = end.Time
	} else if req.Duration != nil {
		if reservation.StartTime.IsZero() {
			reservation.StartTime = time.Now()
		}
		reservation.EndTime = reservation.StartTime.Add(time.Duration(req.Duration.Int()) * time.Second)
	}
	return reservation
}

type slurmRunRequest struct {
	AllocationID string          `json:"allocation_id"`
	JobID        string          `json:"job_id"`
	UserID       string          `json:"user_id"`
	User         string          `json:"user"`
	Partition    string          `json:"partition"`
	Cluster      string          `json:"cluster"`
	Command      []string        `json:"command"`
	CommandLine  string          `json:"command_line"`
	Image        string          `json:"image"`
	Timeout      *slurmTimeLimit `json:"timeout"`
	Time         *slurmTimeLimit `json:"time"`
	TimeLimit    *slurmTimeLimit `json:"time_limit"`
	CPU          int             `json:"cpus"`
	Memory       slurmMemoryMB   `json:"memory"`
	Mem          slurmMemoryMB   `json:"mem"`
	Nodes        int             `json:"nodes"`
	Account      string          `json:"account"`
	QOS          string          `json:"qos"`
	TRES         string          `json:"tres"`
	GRES         string          `json:"gres"`
	Constraint   string          `json:"constraint"`
	Reservation  string          `json:"reservation"`
	NodeList     string          `json:"node_list"`
	Nodelist     string          `json:"nodelist"`
	NodesList    string          `json:"nodeslist"`
	Exclude      string          `json:"exclude"`
	ExcludeNodes string          `json:"exclude_nodes"`
	Exclusive    bool            `json:"exclusive"`
	Root         bool            `json:"root"`
	Network      bool            `json:"network"`
}

type slurmBroadcastRequest struct {
	AllocationID  string            `json:"allocation_id"`
	JobID         string            `json:"job_id"`
	Path          string            `json:"path"`
	Destination   string            `json:"destination"`
	DestPath      string            `json:"dest_path"`
	Content       *string           `json:"content"`
	ContentBase64 *string           `json:"content_base64"`
	Files         map[string]string `json:"files"`
	FilesBase64   map[string]string `json:"files_base64"`
	Mode          string            `json:"mode"`
	Metadata      models.JSONMap    `json:"metadata"`
}

type slurmTriggerRequest struct {
	ID        string `json:"id"`
	TriggerID string `json:"trigger_id"`
	Name      string `json:"name"`
	Event     string `json:"event"`
	Type      string `json:"type"`
	JobID     string `json:"job_id"`
	UserID    string `json:"user_id"`
	User      string `json:"user"`
	Partition string `json:"partition"`
	Cluster   string `json:"cluster"`
	Node      string `json:"node"`
	State     string `json:"state"`
	Action    string `json:"action"`
	Program   string `json:"program"`
	Flags     string `json:"flags"`
	Active    *bool  `json:"active"`
}

type slurmHostlistRequest struct {
	Hostlist  string          `json:"hostlist"`
	NodeList  string          `json:"node_list"`
	Nodelist  string          `json:"nodelist"`
	NodesList string          `json:"nodeslist"`
	Nodes     slurmStringList `json:"nodes"`
	Hostnames slurmStringList `json:"hostnames"`
}

type slurmCronRequest struct {
	ID        string          `json:"id"`
	EntryID   string          `json:"entry_id"`
	Name      string          `json:"name"`
	Schedule  string          `json:"schedule"`
	Enabled   *bool           `json:"enabled"`
	NextRun   *slurmDateTime  `json:"next_run"`
	NextRunAt *slurmDateTime  `json:"next_run_at"`
	Batch     json.RawMessage `json:"batch"`
}

func (req slurmRunRequest) InteractiveRequest() judger.InteractiveRunRequest {
	return req.interactiveRequest(firstNonEmpty(req.AllocationID, req.JobID))
}

func (req slurmRunRequest) interactiveRequest(allocationID string) judger.InteractiveRunRequest {
	timeout := 0
	if req.Timeout != nil {
		timeout = req.Timeout.Int()
	} else if req.Time != nil {
		timeout = req.Time.Int()
	} else if req.TimeLimit != nil {
		timeout = req.TimeLimit.Int()
	}
	return judger.InteractiveRunRequest{
		AllocationID: allocationID,
		Command:      req.Command,
		CommandLine:  req.CommandLine,
		Image:        req.Image,
		Timeout:      timeout,
		CPU:          req.CPU,
		Memory:       firstPositiveMemory(req.Memory, req.Mem),
		Root:         req.Root,
		Network:      req.Network,
	}
}

func (req slurmRunRequest) AllocationRequest() judger.InteractiveAllocationRequest {
	timeLimit := 0
	if req.Timeout != nil {
		timeLimit = req.Timeout.Int()
	} else if req.Time != nil {
		timeLimit = req.Time.Int()
	} else if req.TimeLimit != nil {
		timeLimit = req.TimeLimit.Int()
	}
	cpu := req.CPU
	if cpu <= 0 {
		cpu = 1
	}
	return judger.InteractiveAllocationRequest{
		UserID:       firstNonEmpty(req.UserID, req.User),
		Cluster:      firstNonEmpty(req.Partition, req.Cluster),
		CPU:          cpu,
		Memory:       firstPositiveMemory(req.Memory, req.Mem),
		Nodes:        req.Nodes,
		Account:      req.Account,
		QOS:          req.QOS,
		TRES:         req.TRES,
		GRES:         req.GRES,
		TimeLimit:    timeLimit,
		Constraint:   req.Constraint,
		Reservation:  req.Reservation,
		NodeList:     firstNonEmpty(req.NodeList, req.Nodelist, req.NodesList),
		ExcludeNodes: firstNonEmpty(req.ExcludeNodes, req.Exclude),
		Exclusive:    req.Exclusive,
	}
}

func (h *Handler) slurmSqueue(c *gin.Context) {
	entries, err := h.scheduler.GetQueueSnapshot()
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	records := make([]map[string]interface{}, 0, len(entries))
	for _, entry := range entries {
		matches, err := matchesSlurmQueueFilters(entry, c)
		if err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
		if !matches {
			continue
		}
		records = append(records, slurmQueueRecord(entry))
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "squeue records retrieved")
}

func (h *Handler) slurmSinfo(c *gin.Context) {
	states := h.scheduler.GetClusterStates()
	partitions := make([]string, 0, len(states))
	for partition := range states {
		partitions = append(partitions, partition)
	}
	sort.Strings(partitions)

	records := make([]map[string]interface{}, 0)
	for _, partition := range partitions {
		cluster := states[partition]
		if filter := firstQuery(c, "partition", "cluster"); filter != "" && !containsCSVFold(filter, partition) {
			continue
		}
		nodeNames := make([]string, 0, len(cluster.Nodes))
		for nodeName := range cluster.Nodes {
			nodeNames = append(nodeNames, nodeName)
		}
		sort.Strings(nodeNames)
		for _, nodeName := range nodeNames {
			node := cluster.Nodes[nodeName]
			if filter := firstQuery(c, "node", "nodelist"); filter != "" && !containsCSVFold(filter, nodeName) {
				continue
			}
			record := slurmInfoRecord(partition, cluster.MaxTime, node)
			if !slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(record["state"]), fmt.Sprint(record["native_state"])) {
				continue
			}
			records = append(records, record)
		}
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sinfo records retrieved")
}

func (h *Handler) slurmSacct(c *gin.Context) {
	page, limit := slurmPagination(c, 1, 50, 500)
	offset := (page - 1) * limit
	jobFilter := firstQuery(c, "job_id", "submission_id")
	jobSelectors, err := parseSlurmJobSelectors(jobFilter)
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	filters := slurmAccountingFilters(c)
	query, err := database.BuildAccountingRecordsQuery(h.db, filters)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	if jobSelectors.Has() {
		query = applySlurmJobSelectorQuery(query, "submission_id", jobSelectors)
	}
	var totalItems int64
	if err := query.Count(&totalItems).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	var records []models.AccountingRecord
	if err := query.Order("created_at DESC").Offset(offset).Limit(limit).Find(&records).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	items := make([]map[string]interface{}, 0, len(records))
	for _, record := range records {
		item := slurmAccountingRecord(record)
		if !slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(item["state"])) {
			continue
		}
		items = append(items, item)
	}

	util.Success(c, gin.H{
		"items":        slurmProjectFields(items, slurmFieldsQuery(c)),
		"total_items":  totalItems,
		"total_pages":  int(math.Ceil(float64(totalItems) / float64(limit))),
		"current_page": page,
		"per_page":     limit,
	}, "sacct records retrieved")
}

func (h *Handler) slurmSreport(c *gin.Context) {
	groupBy, err := canonicalSlurmReportGroup(firstQuery(c, "group_by", "group", "by"))
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	jobSelectors, err := parseSlurmJobSelectors(firstQuery(c, "job_id", "submission_id"))
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	filters := slurmAccountingFilters(c)
	query, err := database.BuildAccountingRecordsQuery(h.db, filters)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	if filters["event"] == "" {
		query = query.Where("event IN ?", slurmReportUsageEvents())
	}
	if jobSelectors.Has() {
		query = applySlurmJobSelectorQuery(query, "submission_id", jobSelectors)
	}

	var records []models.AccountingRecord
	if err := query.Order("created_at ASC").Find(&records).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	type aggregate struct {
		Name         string
		Records      int
		JobIDs       map[string]bool
		CPU          int
		Memory       int64
		BillingUnits float64
		StartTime    time.Time
		EndTime      time.Time
	}
	aggregates := make(map[string]*aggregate)
	for _, record := range records {
		item := slurmAccountingRecord(record)
		if !slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(item["state"])) {
			continue
		}
		key := slurmReportGroupKey(record, groupBy)
		agg, ok := aggregates[key]
		if !ok {
			agg = &aggregate{Name: key, JobIDs: make(map[string]bool)}
			aggregates[key] = agg
		}
		agg.Records++
		if record.SubmissionID != "" {
			agg.JobIDs[record.SubmissionID] = true
		}
		agg.CPU += record.CPU
		agg.Memory += record.Memory
		agg.BillingUnits += record.BillingUnits
		if agg.StartTime.IsZero() || record.CreatedAt.Before(agg.StartTime) {
			agg.StartTime = record.CreatedAt
		}
		if agg.EndTime.IsZero() || record.CreatedAt.After(agg.EndTime) {
			agg.EndTime = record.CreatedAt
		}
	}

	keys := make([]string, 0, len(aggregates))
	for key := range aggregates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]map[string]interface{}, 0, len(keys))
	for _, key := range keys {
		agg := aggregates[key]
		row := map[string]interface{}{
			"group_by":      groupBy,
			"name":          agg.Name,
			"records":       agg.Records,
			"jobs":          len(agg.JobIDs),
			"alloc_cpus":    agg.CPU,
			"alloc_mem":     agg.Memory,
			"billing_units": agg.BillingUnits,
			"start_time":    agg.StartTime,
			"end_time":      agg.EndTime,
		}
		row[slurmReportGroupField(groupBy)] = agg.Name
		rows = append(rows, row)
	}
	util.Success(c, slurmProjectFields(rows, slurmFieldsQuery(c)), "sreport records retrieved")
}

func (h *Handler) slurmSeff(c *gin.Context) {
	jobID := strings.TrimSpace(firstNonEmpty(c.Param("id"), firstQuery(c, "job_id", "submission_id", "allocation_id", "id")))
	if jobID == "" {
		util.Error(c, http.StatusBadRequest, "job_id is required")
		return
	}
	stepID := strings.TrimSpace(firstQuery(c, "step_id", "step", "step_name", "job_step_id"))
	record, err := h.slurmEfficiencyRecord(jobID, stepID)
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	util.Success(c, slurmProjectOne(record, slurmFieldsQuery(c)), "seff record retrieved")
}

func (h *Handler) slurmSprio(c *gin.Context) {
	breakdowns, err := h.scheduler.GetPriorityBreakdowns(c.Query("native_status"))
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	records := make([]map[string]interface{}, 0, len(breakdowns))
	for _, item := range breakdowns {
		if jobID := firstQuery(c, "job_id", "id"); jobID != "" {
			jobSelectors, err := parseSlurmJobSelectors(jobID)
			if err != nil {
				util.Error(c, http.StatusBadRequest, err)
				return
			}
			if jobSelectors.Has() && !jobSelectors.Matches(item.JobID, item.ArrayJobID, item.ArrayTaskID) {
				continue
			}
		}
		if arrayJobID := c.Query("array_job_id"); arrayJobID != "" && !containsCSVFold(arrayJobID, item.ArrayJobID) {
			continue
		}
		if arrayTaskID := firstQuery(c, "array_task_id", "array_task"); arrayTaskID != "" && !containsCSVFold(arrayTaskID, strconv.Itoa(item.ArrayTaskID)) {
			continue
		}
		if user := firstQuery(c, "user", "user_id"); user != "" && !containsCSVFold(user, item.UserID) {
			continue
		}
		if partition := firstQuery(c, "partition", "cluster"); partition != "" && !containsCSVFold(partition, item.Partition) {
			continue
		}
		if account := c.Query("account"); account != "" && !containsCSVFold(account, item.Account) {
			continue
		}
		if qos := c.Query("qos"); qos != "" && !containsCSVFold(qos, item.QOS) {
			continue
		}
		if !slurmStateFilterMatches(slurmStateQuery(c), item.SlurmState) {
			continue
		}
		records = append(records, slurmPriorityRecord(item))
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sprio records retrieved")
}

func (h *Handler) slurmSshare(c *gin.Context) {
	shares := h.scheduler.GetFairshareRecords()
	records := make([]map[string]interface{}, 0, len(shares))
	for _, share := range shares {
		if account := c.Query("account"); account != "" && !containsCSVFold(account, share.Account) {
			continue
		}
		records = append(records, slurmFairshareRecord(share))
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sshare records retrieved")
}

func (h *Handler) slurmSdiag(c *gin.Context) {
	entries, err := h.scheduler.GetQueueSnapshot()
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	stateCounts := make(map[string]int)
	nativeStatusCounts := make(map[string]int)
	for _, entry := range entries {
		stateCounts[entry.SlurmState]++
		nativeStatusCounts[string(entry.Status)]++
	}

	clusterStates := h.scheduler.GetClusterStates()
	partitions, totals := slurmDiagnosticPartitions(clusterStates)
	allocationCounts, err := slurmCountByField(h.db, &models.Allocation{}, "status")
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	stepCounts, err := slurmCountByField(h.db, &models.RunStep{}, "status")
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	configSnapshot := h.scheduler.GetSchedulerConfigSnapshot()

	record := map[string]interface{}{
		"generated_at":         time.Now(),
		"queue_size":           configSnapshot.QueueSize,
		"backfill":             configSnapshot.Backfill,
		"queue_lengths":        h.scheduler.GetQueueLengths(),
		"jobs_by_state":        stateCounts,
		"jobs_by_native_state": nativeStatusCounts,
		"active_jobs":          len(entries),
		"partitions":           partitions,
		"partition_count":      len(partitions),
		"nodes":                totals["nodes"],
		"total_cpus":           totals["total_cpus"],
		"allocated_cpus":       totals["allocated_cpus"],
		"idle_cpus":            totals["idle_cpus"],
		"total_memory":         totals["total_memory"],
		"allocated_memory":     totals["allocated_memory"],
		"idle_memory":          totals["idle_memory"],
		"licenses":             slurmLicenseRecords(h.scheduler.GetLicenseStatus()),
		"allocations_by_state": allocationCounts,
		"steps_by_state":       stepCounts,
		"priority_weights":     configSnapshot.PriorityWeights,
		"fairshare_decay":      configSnapshot.FairshareDecay,
	}
	util.Success(c, slurmProjectOne(record, slurmFieldsQuery(c)), "sdiag records retrieved")
}

func (h *Handler) slurmStriggerList(c *gin.Context) {
	query := h.db.Model(&models.SlurmTrigger{}).Order("created_at desc")
	if id := firstQuery(c, "trigger_id", "id"); id != "" {
		query = query.Where("id IN ?", slurmCSVValues(id))
	}
	if name := c.Query("name"); name != "" {
		query = query.Where("name IN ?", slurmCSVValues(name))
	}
	if event := firstQuery(c, "event", "type"); event != "" {
		query = query.Where("event IN ?", slurmCanonicalTriggerEvents(event))
	}
	if active := c.Query("active"); active != "" {
		parsed, err := strconv.ParseBool(active)
		if err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
		query = query.Where("active = ?", parsed)
	}
	var triggers []models.SlurmTrigger
	if err := query.Find(&triggers).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	evaluate := strings.EqualFold(c.Query("evaluate"), "true") || c.Query("evaluate") == "1"
	records := make([]map[string]interface{}, 0, len(triggers))
	for i := range triggers {
		record := slurmTriggerRecord(&triggers[i])
		if evaluate {
			evaluation, err := h.slurmEvaluateTrigger(&triggers[i])
			if err != nil {
				util.Error(c, http.StatusInternalServerError, err)
				return
			}
			record["matched"] = evaluation.Matched
			record["match_count"] = evaluation.Count
			record["message"] = evaluation.Message
		}
		records = append(records, record)
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "strigger records retrieved")
}

func (h *Handler) slurmStriggerUpsert(c *gin.Context) {
	var req slurmTriggerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	trigger, err := slurmTriggerFromRequest(req)
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	if err := h.db.Save(trigger).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	util.Success(c, slurmProjectOne(slurmTriggerRecord(trigger), slurmFieldsQuery(c)), "strigger saved")
}

func (h *Handler) slurmStriggerEvaluate(c *gin.Context) {
	var triggers []models.SlurmTrigger
	query := h.db.Model(&models.SlurmTrigger{}).Where("active = ?", true).Order("created_at asc")
	if id := firstQuery(c, "trigger_id", "id"); id != "" {
		query = query.Where("id IN ?", slurmCSVValues(id))
	}
	if err := query.Find(&triggers).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	records := make([]map[string]interface{}, 0, len(triggers))
	for i := range triggers {
		evaluation, err := h.slurmEvaluateTrigger(&triggers[i])
		if err != nil {
			util.Error(c, http.StatusInternalServerError, err)
			return
		}
		trigger := &triggers[i]
		if evaluation.Matched {
			now := time.Now()
			trigger.FiredAt = &now
			trigger.MatchCount = evaluation.Count
			trigger.Message = evaluation.Message
			if !strings.Contains(strings.ToLower(trigger.Flags), "keep-active") {
				trigger.Active = false
			}
			if err := h.db.Save(trigger).Error; err != nil {
				util.Error(c, http.StatusInternalServerError, err)
				return
			}
		}
		record := slurmTriggerRecord(trigger)
		record["matched"] = evaluation.Matched
		record["match_count"] = evaluation.Count
		record["message"] = evaluation.Message
		records = append(records, record)
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "strigger evaluated")
}

func (h *Handler) slurmStriggerDelete(c *gin.Context) {
	id := firstNonEmpty(c.Param("id"), firstQuery(c, "trigger_id", "id"))
	if strings.TrimSpace(id) == "" {
		util.Error(c, http.StatusBadRequest, "trigger_id is required")
		return
	}
	result := h.db.Where("id IN ?", slurmCSVValues(id)).Delete(&models.SlurmTrigger{})
	if result.Error != nil {
		util.Error(c, http.StatusInternalServerError, result.Error)
		return
	}
	util.Success(c, gin.H{"trigger_id": id, "deleted": result.RowsAffected}, "strigger deleted")
}

func (h *Handler) slurmScrontabList(c *gin.Context) {
	query := h.db.Model(&models.SlurmCronJob{}).Order("created_at desc")
	if id := firstQuery(c, "entry_id", "id"); id != "" {
		query = query.Where("id IN ?", slurmCSVValues(id))
	}
	if user := firstQuery(c, "user", "user_id"); user != "" {
		query = query.Where("user_id IN ?", slurmCSVValues(user))
	}
	if problemID := c.Query("problem_id"); problemID != "" {
		query = query.Where("problem_id IN ?", slurmCSVValues(problemID))
	}
	if enabled := c.Query("enabled"); enabled != "" {
		parsed, err := strconv.ParseBool(enabled)
		if err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
		query = query.Where("enabled = ?", parsed)
	}
	var entries []models.SlurmCronJob
	if err := query.Find(&entries).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	records := make([]map[string]interface{}, 0, len(entries))
	for i := range entries {
		records = append(records, slurmCronRecord(&entries[i]))
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "scrontab records retrieved")
}

func (h *Handler) slurmScrontabUpsert(c *gin.Context) {
	var req slurmCronRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	if id := c.Param("id"); id != "" {
		req.ID = id
	}

	var existing *models.SlurmCronJob
	if id := firstNonEmpty(req.ID, req.EntryID); id != "" {
		var loaded models.SlurmCronJob
		err := h.db.Where("id = ?", id).First(&loaded).Error
		if err == nil {
			existing = &loaded
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			util.Error(c, http.StatusInternalServerError, err)
			return
		}
	}

	entry, err := slurmCronFromRequest(req, existing, time.Now())
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	if err := h.db.Save(entry).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	util.Success(c, slurmProjectOne(slurmCronRecord(entry), slurmFieldsQuery(c)), "scrontab saved")
}

func (h *Handler) slurmScrontabEvaluate(c *gin.Context) {
	now := time.Now()
	query := h.db.Model(&models.SlurmCronJob{}).Where("enabled = ?", true).Order("next_run_at asc")
	if id := firstQuery(c, "entry_id", "id"); id != "" {
		query = query.Where("id IN ?", slurmCSVValues(id))
	}
	var entries []models.SlurmCronJob
	if err := query.Find(&entries).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	records := make([]map[string]interface{}, 0, len(entries))
	for i := range entries {
		entry := &entries[i]
		if entry.NextRunAt == nil {
			nextRun, err := slurmCronNextRun(entry.Schedule, now)
			if err != nil {
				entry.Message = err.Error()
				h.db.Save(entry)
				records = append(records, slurmCronEvaluationRecord(entry, false, nil, err))
				continue
			}
			entry.NextRunAt = &nextRun
			h.db.Save(entry)
		}
		if entry.NextRunAt != nil && entry.NextRunAt.After(now) {
			records = append(records, slurmCronEvaluationRecord(entry, false, nil, nil))
			continue
		}

		var batchReq slurmBatchRequest
		if err := json.Unmarshal([]byte(entry.BatchJSON), &batchReq); err != nil {
			entry.Message = fmt.Sprintf("invalid batch json: %v", err)
			h.db.Save(entry)
			records = append(records, slurmCronEvaluationRecord(entry, false, nil, err))
			continue
		}
		response, _, err := h.submitSlurmBatch(batchReq)
		if err != nil {
			entry.Message = err.Error()
			h.db.Save(entry)
			records = append(records, slurmCronEvaluationRecord(entry, false, nil, err))
			continue
		}

		nextRun, err := slurmCronNextRun(entry.Schedule, now)
		if err != nil {
			entry.Message = err.Error()
			h.db.Save(entry)
			records = append(records, slurmCronEvaluationRecord(entry, true, response, err))
			continue
		}
		entry.LastRunAt = &now
		entry.NextRunAt = &nextRun
		entry.LastJobID = fmt.Sprint(response["job_id"])
		entry.RunCount++
		entry.Message = "submitted"
		if err := h.db.Save(entry).Error; err != nil {
			util.Error(c, http.StatusInternalServerError, err)
			return
		}
		records = append(records, slurmCronEvaluationRecord(entry, true, response, nil))
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "scrontab evaluated")
}

func (h *Handler) slurmScrontabDelete(c *gin.Context) {
	id := firstNonEmpty(c.Param("id"), firstQuery(c, "entry_id", "id"))
	if strings.TrimSpace(id) == "" {
		util.Error(c, http.StatusBadRequest, "entry_id is required")
		return
	}
	result := h.db.Where("id IN ?", slurmCSVValues(id)).Delete(&models.SlurmCronJob{})
	if result.Error != nil {
		util.Error(c, http.StatusInternalServerError, result.Error)
		return
	}
	util.Success(c, gin.H{"entry_id": id, "deleted": result.RowsAffected}, "scrontab deleted")
}

func (h *Handler) slurmSacctmgrPing(c *gin.Context) {
	responding, message := h.slurmDatabaseStatus()
	status := "UP"
	if !responding {
		status = "DOWN"
	}
	record := map[string]interface{}{
		"generated_at": time.Now(),
		"daemon":       "slurmdbd",
		"service":      "database",
		"role":         "accounting_storage",
		"status":       status,
		"responding":   responding,
		"primary":      true,
		"message":      message,
	}
	util.Success(c, slurmProjectOne(record, slurmFieldsQuery(c)), "sacctmgr ping retrieved")
}

func (h *Handler) slurmSacctmgrShowAccounts(c *gin.Context) {
	accounts := h.scheduler.ListAccounts(c.Query("account"))
	records := make([]map[string]interface{}, 0, len(accounts))
	for _, account := range accounts {
		records = append(records, slurmAccountRecord(account))
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sacctmgr accounts retrieved")
}

func (h *Handler) slurmSacctmgrUpsertAccount(c *gin.Context) {
	var req slurmAccountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	account, err := h.scheduler.UpsertAccount(req.Config(c.Param("name")))
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	util.Success(c, slurmProjectOne(slurmAccountRecord(account), slurmFieldsQuery(c)), "sacctmgr account saved")
}

func (h *Handler) slurmSacctmgrDeleteAccount(c *gin.Context) {
	if err := h.scheduler.DeleteAccount(c.Param("name")); err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	util.Success(c, gin.H{"account": c.Param("name"), "deleted": true}, "sacctmgr account deleted")
}

func (h *Handler) slurmSacctmgrShowUsers(c *gin.Context) {
	associations := h.scheduler.ListAssociations(firstQuery(c, "account", "default_account"), firstQuery(c, "user", "user_id", "name"), c.Query("qos"))
	records, err := h.slurmUserRecords(associations)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sacctmgr users retrieved")
}

func (h *Handler) slurmSacctmgrShowClusters(c *gin.Context) {
	records := h.slurmClusterRecords()
	if clusterFilter := firstQuery(c, "cluster", "name", "partition"); clusterFilter != "" {
		filtered := make([]map[string]interface{}, 0, len(records))
		for _, record := range records {
			if containsCSVFold(clusterFilter, fmt.Sprint(record["cluster"])) || containsCSVFold(clusterFilter, fmt.Sprint(record["name"])) {
				filtered = append(filtered, record)
			}
		}
		records = filtered
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sacctmgr clusters retrieved")
}

func (h *Handler) slurmSacctmgrShowConfig(c *gin.Context) {
	util.Success(c, slurmProjectOne(h.slurmSacctmgrConfigRecord(), slurmFieldsQuery(c)), "sacctmgr config retrieved")
}

func (h *Handler) slurmSacctmgrShowStats(c *gin.Context) {
	record, err := h.slurmSacctmgrStatsRecord()
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	util.Success(c, slurmProjectOne(record, slurmFieldsQuery(c)), "sacctmgr stats retrieved")
}

func (h *Handler) slurmSacctmgrShowJobs(c *gin.Context) {
	submissions, err := h.slurmFilteredSubmissions(c)
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	ids := make([]string, 0, len(submissions))
	for _, sub := range submissions {
		ids = append(ids, sub.ID)
	}
	summaries, err := h.slurmAccountingJobSummaries(ids)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	records := make([]map[string]interface{}, 0, len(submissions))
	for i := range submissions {
		submissions[i].PopulateSlurmState()
		record := slurmSacctmgrJobRecord(&submissions[i], summaries[submissions[i].ID])
		if !slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(record["state"]), fmt.Sprint(record["native_status"])) {
			continue
		}
		records = append(records, record)
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sacctmgr jobs retrieved")
}

func (h *Handler) slurmSacctmgrShowProblems(c *gin.Context) {
	records, err := h.slurmProblemRecords()
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	filtered := make([]map[string]interface{}, 0, len(records))
	for _, record := range records {
		if problem := firstQuery(c, "problem", "problem_id", "name"); problem != "" &&
			!containsCSVFold(problem, fmt.Sprint(record["problem_id"])) &&
			!containsCSVFold(problem, fmt.Sprint(record["name"])) {
			continue
		}
		if partition := firstQuery(c, "partition", "cluster"); partition != "" && !containsCSVFold(partition, fmt.Sprint(record["partition"])) {
			continue
		}
		if account := c.Query("account"); account != "" && !containsCSVFold(account, fmt.Sprint(record["account"])) {
			continue
		}
		if qos := c.Query("qos"); qos != "" && !containsCSVFold(qos, fmt.Sprint(record["qos"])) {
			continue
		}
		if state := slurmStateQuery(c); state != "" && !slurmStateFilterMatches(state, fmt.Sprint(record["state"])) {
			continue
		}
		filtered = append(filtered, record)
	}
	util.Success(c, slurmProjectFields(filtered, slurmFieldsQuery(c)), "sacctmgr problems retrieved")
}

func (h *Handler) slurmSacctmgrShowResources(c *gin.Context) {
	records := h.slurmResourceRecords()
	if resourceFilter := firstQuery(c, "resource", "name", "tres"); resourceFilter != "" {
		filtered := make([]map[string]interface{}, 0, len(records))
		for _, record := range records {
			if containsCSVFold(resourceFilter, fmt.Sprint(record["resource"])) ||
				containsCSVFold(resourceFilter, fmt.Sprint(record["name"])) ||
				containsCSVFold(resourceFilter, fmt.Sprint(record["tres"])) {
				filtered = append(filtered, record)
			}
		}
		records = filtered
	}
	if typ := c.Query("type"); typ != "" {
		filtered := make([]map[string]interface{}, 0, len(records))
		for _, record := range records {
			if containsCSVFold(typ, fmt.Sprint(record["type"])) {
				filtered = append(filtered, record)
			}
		}
		records = filtered
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sacctmgr resources retrieved")
}

func (h *Handler) slurmSacctmgrShowRunawayJobs(c *gin.Context) {
	records, err := h.slurmRunawayJobRecords(c)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sacctmgr runaway jobs retrieved")
}

func (h *Handler) slurmSacctmgrShowTransactions(c *gin.Context) {
	records, err := h.slurmTransactionRecords(c)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sacctmgr transactions retrieved")
}

func (h *Handler) slurmSacctmgrShowEvents(c *gin.Context) {
	records, err := h.slurmEventRecords(c)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sacctmgr events retrieved")
}

func (h *Handler) slurmSacctmgrUpsertUser(c *gin.Context) {
	var req slurmUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	user := strings.TrimSpace(firstNonEmpty(c.Param("name"), req.User, req.UserID, req.Name))
	account := strings.TrimSpace(firstNonEmpty(req.DefaultAccount, req.Account))
	if user == "" {
		util.Error(c, http.StatusBadRequest, "user is required")
		return
	}
	if account == "" {
		util.Error(c, http.StatusBadRequest, "account or default_account is required")
		return
	}

	qosItems := mergeSlurmStringLists(req.QOS, req.AllowQOS, req.AllowedQOS)
	if len(qosItems) == 0 {
		qosItems = []string{""}
	}
	for _, qos := range qosItems {
		if _, err := h.scheduler.UpsertAssociation(judger.AssociationUpdate{
			Account:       account,
			User:          user,
			QOS:           qos,
			Fairshare:     req.Fairshare,
			MaxJobs:       req.MaxJobs,
			MaxSubmit:     req.MaxSubmit,
			MaxBillingRun: req.MaxBillingRunning,
			MaxBillingSub: req.MaxBillingSubmit,
		}); err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
	}

	records, err := h.slurmUserRecords(h.scheduler.ListAssociations(account, user, ""))
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	if len(records) == 0 {
		util.Error(c, http.StatusInternalServerError, "sacctmgr user was saved but could not be reloaded")
		return
	}
	util.Success(c, slurmProjectOne(records[0], slurmFieldsQuery(c)), "sacctmgr user saved")
}

func (h *Handler) slurmSacctmgrDeleteUser(c *gin.Context) {
	user := strings.TrimSpace(firstNonEmpty(c.Param("name"), firstQuery(c, "user", "user_id", "name")))
	account := strings.TrimSpace(firstQuery(c, "account", "default_account"))
	qos := strings.TrimSpace(c.Query("qos"))
	if user == "" {
		util.Error(c, http.StatusBadRequest, "user is required")
		return
	}

	candidates := h.scheduler.ListAssociations(account, user, qos)
	if len(candidates) == 0 {
		util.Error(c, http.StatusNotFound, fmt.Sprintf("user %q association not found", user))
		return
	}
	if account == "" {
		accounts := make(map[string]bool)
		for _, association := range candidates {
			accounts[association.Account] = true
		}
		if len(accounts) != 1 {
			util.Error(c, http.StatusBadRequest, "account or default_account is required when user belongs to multiple accounts")
			return
		}
		for accountName := range accounts {
			account = accountName
		}
	}

	deleted, err := h.scheduler.DeleteAssociation(account, user, qos)
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	record := slurmAssociationRecord(deleted)
	record["deleted"] = true
	util.Success(c, slurmProjectOne(record, slurmFieldsQuery(c)), "sacctmgr user deleted")
}

func (h *Handler) slurmSacctmgrShowQOS(c *gin.Context) {
	qosItems := h.scheduler.ListQOS(c.Query("qos"))
	records := make([]map[string]interface{}, 0, len(qosItems))
	for _, qos := range qosItems {
		records = append(records, slurmQOSRecord(qos))
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sacctmgr qos retrieved")
}

func (h *Handler) slurmSacctmgrShowTRES(c *gin.Context) {
	records := h.slurmTRESRecords()
	if tresFilter := firstQuery(c, "tres", "name"); tresFilter != "" {
		values := slurmCSVValues(tresFilter)
		filtered := make([]map[string]interface{}, 0, len(records))
		for _, record := range records {
			if stringInFoldList(fmt.Sprint(record["tres"]), values) || stringInFoldList(fmt.Sprint(record["name"]), values) {
				filtered = append(filtered, record)
			}
		}
		records = filtered
	}
	if typ := c.Query("type"); typ != "" {
		values := slurmCSVValues(typ)
		filtered := make([]map[string]interface{}, 0, len(records))
		for _, record := range records {
			if stringInFoldList(fmt.Sprint(record["type"]), values) {
				filtered = append(filtered, record)
			}
		}
		records = filtered
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sacctmgr tres retrieved")
}

func (h *Handler) slurmSacctmgrUpsertQOS(c *gin.Context) {
	var req slurmQOSRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	qos, err := h.scheduler.UpsertQOS(req.Config(c.Param("name")))
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	util.Success(c, slurmProjectOne(slurmQOSRecord(qos), slurmFieldsQuery(c)), "sacctmgr qos saved")
}

func (h *Handler) slurmSacctmgrDeleteQOS(c *gin.Context) {
	if err := h.scheduler.DeleteQOS(c.Param("name")); err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	util.Success(c, gin.H{"qos": c.Param("name"), "deleted": true}, "sacctmgr qos deleted")
}

func (h *Handler) slurmSacctmgrShowAssociations(c *gin.Context) {
	associations := h.scheduler.ListAssociations(c.Query("account"), firstQuery(c, "user", "user_id"), c.Query("qos"))
	records := make([]map[string]interface{}, 0, len(associations))
	for _, association := range associations {
		records = append(records, slurmAssociationRecord(association))
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sacctmgr associations retrieved")
}

func (h *Handler) slurmSacctmgrUpsertAssociation(c *gin.Context) {
	var req struct {
		Account           string   `json:"account"`
		ParentAccount     *string  `json:"parent_account"`
		User              string   `json:"user"`
		UserID            string   `json:"user_id"`
		QOS               string   `json:"qos"`
		Fairshare         *int     `json:"fairshare"`
		MaxJobs           *int     `json:"max_jobs"`
		MaxSubmit         *int     `json:"max_submit"`
		MaxBillingRunning *float64 `json:"max_billing_running"`
		MaxBillingSubmit  *float64 `json:"max_billing_submit"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	if account := c.Param("account"); account != "" {
		req.Account = account
	}
	association, err := h.scheduler.UpsertAssociation(judger.AssociationUpdate{
		Account:       req.Account,
		ParentAccount: req.ParentAccount,
		User:          firstNonEmpty(req.User, req.UserID),
		QOS:           req.QOS,
		Fairshare:     req.Fairshare,
		MaxJobs:       req.MaxJobs,
		MaxSubmit:     req.MaxSubmit,
		MaxBillingRun: req.MaxBillingRunning,
		MaxBillingSub: req.MaxBillingSubmit,
	})
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	util.Success(c, slurmProjectOne(slurmAssociationRecord(association), slurmFieldsQuery(c)), "sacctmgr association saved")
}

func (h *Handler) slurmSacctmgrDeleteAssociation(c *gin.Context) {
	association, err := h.scheduler.DeleteAssociation(c.Param("account"), firstQuery(c, "user", "user_id"), c.Query("qos"))
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	util.Success(c, slurmProjectOne(slurmAssociationRecord(association), slurmFieldsQuery(c)), "sacctmgr association deleted")
}

func (h *Handler) slurmSubmitBatch(c *gin.Context) {
	var req slurmBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	response, status, err := h.submitSlurmBatch(req)
	if err != nil {
		util.Error(c, status, err)
		return
	}
	util.Success(c, slurmProjectOne(response, slurmFieldsQuery(c)), "batch job submitted")
}

func (h *Handler) submitSlurmBatch(req slurmBatchRequest) (gin.H, int, error) {
	normalizeSlurmBatchRequestAliases(&req)
	if strings.TrimSpace(req.UserID) == "" || strings.TrimSpace(req.ProblemID) == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("user_id and problem_id are required")
	}
	applySlurmBatchWrap(&req)
	if err := applySlurmScriptDirectives(&req); err != nil {
		return nil, http.StatusBadRequest, err
	}
	applySlurmBatchIODefaults(&req)
	if err := applySlurmBatchEnvironment(&req); err != nil {
		return nil, http.StatusBadRequest, err
	}
	applySlurmBatchResourceShape(&req)
	if err := validateSlurmBatchResources(req); err != nil {
		return nil, http.StatusBadRequest, err
	}
	if _, err := database.GetUserByID(h.db, req.UserID); err != nil {
		return nil, http.StatusNotFound, fmt.Errorf("user not found")
	}

	h.appState.RLock()
	problem, ok := h.appState.Problems[req.ProblemID]
	h.appState.RUnlock()
	if !ok {
		return nil, http.StatusNotFound, fmt.Errorf("problem not found")
	}

	arraySpec := req.Array
	if arraySpec == "" {
		arraySpec = problem.Scheduling.Array
	}
	jobArray, err := judger.ParseJobArray(arraySpec)
	if err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("invalid job array: %w", err)
	}
	taskIDs := jobArray.TaskIDs
	if len(taskIDs) == 0 {
		taskIDs = []int{0}
	}

	arrayJobID := uuid.NewString()
	basePath := filepath.Join(h.cfg.Storage.SubmissionContent, arrayJobID)
	if err := writeSlurmBatchFiles(basePath, req); err != nil {
		return nil, http.StatusBadRequest, err
	}

	submissions := make([]models.Submission, 0, len(taskIDs))
	for i, taskID := range taskIDs {
		submissionID := arrayJobID
		if i > 0 {
			submissionID = uuid.NewString()
			if err := judger.CopyDir(basePath, filepath.Join(h.cfg.Storage.SubmissionContent, submissionID)); err != nil {
				os.RemoveAll(filepath.Join(h.cfg.Storage.SubmissionContent, submissionID))
				return nil, http.StatusInternalServerError, fmt.Errorf("failed to prepare array task files: %w", err)
			}
		}

		sub := models.Submission{
			ID:        submissionID,
			ProblemID: req.ProblemID,
			UserID:    req.UserID,
			Status:    models.StatusQueued,
			Cluster:   problem.Cluster,
			IsValid:   true,
		}
		judger.ApplyProblemScheduling(&sub, problem)
		applySlurmBatchScheduling(&sub, req)
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

	if err := h.db.Transaction(func(tx *gorm.DB) error {
		for i := range submissions {
			if err := database.CreateSubmission(tx, &submissions[i]); err != nil {
				return err
			}
			if err := database.RecordAccounting(tx, database.AccountingFromSubmission(&submissions[i], database.AccountEventSubmitted)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create batch submission: %w", err)
	}

	submissionIDs := make([]string, 0, len(submissions))
	taskIDResponse := make([]int, 0, len(submissions))
	for i := range submissions {
		submissionIDs = append(submissionIDs, submissions[i].ID)
		taskIDResponse = append(taskIDResponse, submissions[i].ArrayTaskID)
		h.scheduler.Submit(&submissions[i], problem)
	}

	response := gin.H{
		"job_id":             submissionIDs[0],
		"submission_id":      submissionIDs[0],
		"name":               slurmSubmissionJobName(&submissions[0]),
		"job_name":           slurmSubmissionJobName(&submissions[0]),
		"problem_id":         submissions[0].ProblemID,
		"export":             submissions[0].ExportEnv,
		"environment":        submissions[0].Environment,
		"state":              models.SlurmStatePending,
		"partition":          submissions[0].Cluster,
		"cpus":               submissions[0].CPU,
		"ntasks":             submissions[0].NTasks,
		"cpus_per_task":      submissions[0].CPUsPerTask,
		"nodes":              slurmJobNodeCount(submissions[0].Nodes),
		"requested_nodelist": submissions[0].NodeList,
		"exclude_nodes":      submissions[0].ExcludeNodes,
		"licenses":           submissions[0].Licenses,
	}
	if jobArray.Spec != "" {
		response["array_job_id"] = arrayJobID
		response["submission_ids"] = submissionIDs
		response["task_ids"] = taskIDResponse
		response["array_max_running"] = jobArray.MaxRunning
	}
	return response, http.StatusOK, nil
}

func (h *Handler) slurmCreateAllocation(c *gin.Context) {
	var req slurmAllocationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	allocation, err := h.scheduler.AllocateInteractive(req.InteractiveRequest())
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	util.Success(c, slurmProjectOne(slurmAllocationRecord(allocation), slurmFieldsQuery(c)), "allocation created")
}

func (h *Handler) slurmListAllocations(c *gin.Context) {
	allocations, err := h.scheduler.ListInteractiveAllocations(c.Query("status"))
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	records := make([]map[string]interface{}, 0, len(allocations))
	for i := range allocations {
		records = append(records, slurmAllocationRecord(&allocations[i]))
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "allocations retrieved")
}

func (h *Handler) slurmShowAllocation(c *gin.Context) {
	allocation, err := h.scheduler.GetInteractiveAllocation(c.Param("id"))
	if err != nil {
		util.Error(c, http.StatusNotFound, "allocation not found")
		return
	}
	util.Success(c, slurmProjectOne(slurmAllocationRecord(allocation), slurmFieldsQuery(c)), "allocation retrieved")
}

func (h *Handler) slurmReleaseAllocation(c *gin.Context) {
	allocation, err := h.scheduler.ReleaseInteractiveAllocation(c.Param("id"))
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	util.Success(c, slurmProjectOne(slurmAllocationRecord(allocation), slurmFieldsQuery(c)), "allocation released")
}

func (h *Handler) slurmSbcast(c *gin.Context) {
	var req slurmBroadcastRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	allocationID := firstNonEmpty(req.AllocationID, req.JobID)
	if strings.TrimSpace(allocationID) == "" {
		util.Error(c, http.StatusBadRequest, "allocation_id is required")
		return
	}
	allocation, err := h.scheduler.GetInteractiveAllocation(allocationID)
	if err != nil {
		util.Error(c, http.StatusNotFound, "allocation not found")
		return
	}
	if allocation.Status != models.AllocationActive {
		util.Error(c, http.StatusBadRequest, fmt.Sprintf("allocation %s is not active", allocation.ID))
		return
	}

	files, err := slurmBroadcastFiles(req)
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	if len(files) == 0 {
		util.Error(c, http.StatusBadRequest, "at least one file is required")
		return
	}

	records := make([]map[string]interface{}, 0, len(files))
	totalBytes := 0
	destinations := make([]string, 0, len(files))
	for destination := range files {
		destinations = append(destinations, destination)
	}
	sort.Strings(destinations)
	for _, destination := range destinations {
		data := files[destination]
		containerPath, err := judger.WriteInteractiveBroadcastFile(h.cfg.Storage.SubmissionContent, allocation.ID, destination, data)
		if err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
		totalBytes += len(data)
		records = append(records, map[string]interface{}{
			"destination":    destination,
			"container_path": containerPath,
			"bytes":          len(data),
		})
	}

	util.Success(c, slurmProjectOne(gin.H{
		"allocation_id": allocation.ID,
		"job_id":        allocation.ID,
		"state":         "BROADCASTED",
		"files":         records,
		"file_count":    len(records),
		"bytes":         totalBytes,
		"staging_dir":   judger.InteractiveBroadcastDir(h.cfg.Storage.SubmissionContent, allocation.ID),
	}, slurmFieldsQuery(c)), "sbcast files staged")
}

func (h *Handler) slurmRun(c *gin.Context) {
	var req slurmRunRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(firstNonEmpty(req.AllocationID, req.JobID)) == "" {
		h.slurmRunWithImplicitAllocation(c, req)
		return
	}
	step, err := h.scheduler.RunInteractiveStep(req.InteractiveRequest())
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	util.Success(c, slurmProjectOne(slurmRunStepRecord(step), slurmFieldsQuery(c)), "srun step completed")
}

func (h *Handler) slurmRunWithImplicitAllocation(c *gin.Context, req slurmRunRequest) {
	allocReq := req.AllocationRequest()
	if strings.TrimSpace(allocReq.UserID) == "" {
		util.Error(c, http.StatusBadRequest, "user_id is required when allocation_id is omitted")
		return
	}
	allocation, err := h.scheduler.AllocateInteractive(allocReq)
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}

	step, runErr := h.scheduler.RunInteractiveStep(req.interactiveRequest(allocation.ID))
	released, releaseErr := h.scheduler.ReleaseInteractiveAllocation(allocation.ID)
	if releaseErr != nil {
		zap.S().Warnf("failed to release implicit srun allocation %s: %v", allocation.ID, releaseErr)
	}
	if runErr != nil {
		util.Error(c, http.StatusBadRequest, runErr)
		return
	}
	if releaseErr != nil {
		util.Error(c, http.StatusInternalServerError, releaseErr)
		return
	}

	record := slurmRunStepRecord(step)
	record["implicit_allocation"] = true
	record["allocation_released"] = released != nil && released.Status == models.AllocationReleased
	util.Success(c, slurmProjectOne(record, slurmFieldsQuery(c)), "srun step completed")
}

func (h *Handler) slurmListRunSteps(c *gin.Context) {
	steps, err := h.scheduler.ListInteractiveRunSteps(firstQuery(c, "allocation_id", "job_id"), firstQuery(c, "status", "native_status"))
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	records := make([]map[string]interface{}, 0, len(steps))
	for i := range steps {
		record := slurmRunStepRecord(&steps[i])
		if !slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(record["state"]), fmt.Sprint(record["native_status"])) {
			continue
		}
		records = append(records, record)
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "srun steps retrieved")
}

func (h *Handler) slurmShowRunStep(c *gin.Context) {
	step, err := h.scheduler.GetInteractiveRunStep(c.Param("id"))
	if err != nil {
		util.Error(c, http.StatusNotFound, "srun step not found")
		return
	}
	util.Success(c, slurmProjectOne(slurmRunStepRecord(step), slurmFieldsQuery(c)), "srun step retrieved")
}

func (h *Handler) slurmSattach(c *gin.Context) {
	stepID := slurmAttachStepID(firstNonEmpty(c.Param("id"), firstQuery(c, "step_id", "id", "job_step_id")))
	if stepID != "" {
		step, err := h.scheduler.GetInteractiveRunStep(stepID)
		if err != nil {
			util.Error(c, http.StatusNotFound, "sattach step not found")
			return
		}
		util.Success(c, slurmProjectOne(slurmAttachRecord(step), slurmFieldsQuery(c)), "sattach step retrieved")
		return
	}

	steps, err := h.scheduler.ListInteractiveRunSteps(firstQuery(c, "allocation_id", "job_id"), firstQuery(c, "status", "native_status"))
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	records := make([]map[string]interface{}, 0, len(steps))
	for i := range steps {
		step := &steps[i]
		record := slurmAttachRecord(step)
		if !slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(record["state"]), fmt.Sprint(record["native_status"])) {
			continue
		}
		if user := firstQuery(c, "user", "user_id"); user != "" && !containsCSVFold(user, step.UserID) {
			continue
		}
		records = append(records, record)
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sattach steps retrieved")
}

func (h *Handler) slurmSstat(c *gin.Context) {
	steps, err := h.scheduler.ListInteractiveRunSteps(firstQuery(c, "allocation_id", "job_id"), firstQuery(c, "status", "native_status"))
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	records := make([]map[string]interface{}, 0, len(steps))
	for i := range steps {
		step := &steps[i]
		record := slurmStepStatRecord(step)
		if stepID := firstQuery(c, "step_id", "id"); stepID != "" && step.ID != stepID {
			continue
		}
		if !slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(record["state"]), fmt.Sprint(record["native_status"])) {
			continue
		}
		if user := firstQuery(c, "user", "user_id"); user != "" && !containsCSVFold(user, step.UserID) {
			continue
		}
		records = append(records, record)
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "sstat records retrieved")
}

func (h *Handler) slurmShowStepStat(c *gin.Context) {
	step, err := h.scheduler.GetInteractiveRunStep(c.Param("id"))
	if err != nil {
		util.Error(c, http.StatusNotFound, "sstat step not found")
		return
	}
	util.Success(c, slurmProjectOne(slurmStepStatRecord(step), slurmFieldsQuery(c)), "sstat step retrieved")
}

func (h *Handler) slurmShowJobs(c *gin.Context) {
	query := h.db.Model(&models.Submission{}).Order("created_at desc")
	if jobID := firstQuery(c, "job_id", "id"); jobID != "" {
		jobSelectors, err := parseSlurmJobSelectors(jobID)
		if err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
		query = applySlurmJobSelectorQuery(query, "id", jobSelectors)
	}
	if arrayJobID := c.Query("array_job_id"); arrayJobID != "" {
		query = query.Where("array_job_id IN ?", slurmCSVValues(arrayJobID))
	}
	if arrayTaskID := firstQuery(c, "array_task_id", "array_task"); arrayTaskID != "" {
		taskIDs, err := slurmCSVInts(arrayTaskID)
		if err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
		query = query.Where("array_task_id IN ?", taskIDs)
	}
	if userID := firstQuery(c, "user", "user_id"); userID != "" {
		query = query.Where("user_id IN ?", slurmCSVValues(userID))
	}
	if partition := firstQuery(c, "partition", "cluster"); partition != "" {
		query = query.Where("cluster IN ?", slurmCSVValues(partition))
	}
	if jobName := firstQuery(c, "job_name", "name"); jobName != "" {
		jobNames := slurmCSVValues(jobName)
		query = query.Where("job_name IN ? OR (job_name = '' AND problem_id IN ?)", jobNames, jobNames)
	}
	if account := c.Query("account"); account != "" {
		query = query.Where("account IN ?", slurmCSVValues(account))
	}
	if qos := c.Query("qos"); qos != "" {
		query = query.Where("qos IN ?", slurmCSVValues(qos))
	}
	if status := firstQuery(c, "status", "native_status"); status != "" {
		query = query.Where("status IN ?", slurmCSVValues(status))
	}

	var submissions []models.Submission
	if err := query.Find(&submissions).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	records := make([]map[string]interface{}, 0, len(submissions))
	for i := range submissions {
		submissions[i].PopulateSlurmState()
		record := slurmJobRecord(&submissions[i])
		if !slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(record["state"]), fmt.Sprint(record["native_status"])) {
			continue
		}
		records = append(records, record)
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "scontrol show jobs retrieved")
}

func (h *Handler) slurmShowJob(c *gin.Context) {
	sub, err := h.getSlurmJobBySelector(c.Param("id"))
	if err != nil {
		util.Error(c, http.StatusNotFound, "job not found")
		return
	}
	sub.PopulateSlurmState()
	record := slurmJobRecord(sub)
	util.Success(c, slurmProjectOne(record, slurmFieldsQuery(c)), "scontrol show job retrieved")
}

func (h *Handler) getSlurmJobBySelector(selector string) (*models.Submission, error) {
	if sub, err := database.GetSubmission(h.db, selector); err == nil {
		return sub, nil
	}

	jobSelectors, err := parseSlurmJobSelectors(selector)
	if err != nil {
		return nil, err
	}
	if !jobSelectors.Has() {
		return database.GetSubmission(h.db, selector)
	}

	query := applySlurmJobSelectorQuery(h.db.Model(&models.Submission{}), "id", jobSelectors).Order("created_at desc")
	var sub models.Submission
	if err := query.First(&sub).Error; err != nil {
		return nil, err
	}
	return &sub, nil
}

func (h *Handler) slurmShowHostnames(c *gin.Context) {
	req, err := bindSlurmHostlistRequest(c)
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	hostlist := slurmHostlistText(req)
	var hostnames []string
	if hostlist == "" {
		hostnames = h.slurmConfiguredHostnames()
		hostlist = slurmCompressHostnames(hostnames)
	} else {
		hostnames, err = slurmExpandHostlist(hostlist)
		if err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
	}
	util.Success(c, slurmProjectOne(slurmHostlistRecord(hostlist, hostnames), slurmFieldsQuery(c)), "scontrol show hostnames retrieved")
}

func (h *Handler) slurmShowHostlist(c *gin.Context) {
	req, err := bindSlurmHostlistRequest(c)
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	hostnames := append([]string(nil), req.Hostnames.Strings()...)
	hostnames = append(hostnames, req.Nodes.Strings()...)
	if len(hostnames) == 0 {
		hostlist := firstNonEmpty(req.Hostlist, req.NodeList, req.Nodelist, req.NodesList)
		if hostlist != "" {
			hostnames, err = slurmExpandHostlist(hostlist)
			if err != nil {
				util.Error(c, http.StatusBadRequest, err)
				return
			}
		}
	}
	if len(hostnames) > 0 {
		hostnames, err = slurmExpandHostlist(strings.Join(hostnames, ","))
		if err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
	}
	if len(hostnames) == 0 {
		hostnames = h.slurmConfiguredHostnames()
	}
	hostlist := slurmCompressHostnames(hostnames)
	util.Success(c, slurmProjectOne(slurmHostlistRecord(hostlist, slurmUniqueHostnames(hostnames)), slurmFieldsQuery(c)), "scontrol show hostlist retrieved")
}

func (h *Handler) slurmShowSteps(c *gin.Context) {
	steps, err := h.scheduler.ListInteractiveRunSteps(firstQuery(c, "allocation_id", "job_id"), firstQuery(c, "status", "native_status"))
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	records := make([]map[string]interface{}, 0, len(steps))
	for i := range steps {
		step := &steps[i]
		record := slurmRunStepRecord(step)
		if !matchesSlurmStepFilters(step, record, c) {
			continue
		}
		records = append(records, record)
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "scontrol show steps retrieved")
}

func (h *Handler) slurmShowStep(c *gin.Context) {
	stepID := slurmAttachStepID(c.Param("id"))
	step, err := h.scheduler.GetInteractiveRunStep(stepID)
	if err != nil {
		util.Error(c, http.StatusNotFound, "step not found")
		return
	}
	record := slurmRunStepRecord(step)
	util.Success(c, slurmProjectOne(record, slurmFieldsQuery(c)), "scontrol show step retrieved")
}

func (h *Handler) slurmShowNodes(c *gin.Context) {
	states := h.scheduler.GetClusterStates()
	partitions := make([]string, 0, len(states))
	for partition := range states {
		partitions = append(partitions, partition)
	}
	sort.Strings(partitions)

	records := make([]map[string]interface{}, 0)
	for _, partition := range partitions {
		if filter := firstQuery(c, "partition", "cluster"); filter != "" && !containsCSVFold(filter, partition) {
			continue
		}
		cluster := states[partition]
		nodeNames := make([]string, 0, len(cluster.Nodes))
		for nodeName := range cluster.Nodes {
			nodeNames = append(nodeNames, nodeName)
		}
		sort.Strings(nodeNames)
		for _, nodeName := range nodeNames {
			if filter := firstQuery(c, "node", "nodelist"); filter != "" && !containsCSVFold(filter, nodeName) {
				continue
			}
			node := cluster.Nodes[nodeName]
			nodeConfigCopy := *node.Node
			detail := &judger.NodeDetail{
				Node:       &nodeConfigCopy,
				UsedMemory: node.UsedMemory,
				UsedCores:  append([]bool(nil), node.UsedCores...),
				IsPaused:   node.IsPaused,
			}
			record := slurmNodeRecord(partition, detail)
			if !slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(record["state"]), fmt.Sprint(record["native_state"])) {
				continue
			}
			records = append(records, record)
		}
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "scontrol show nodes retrieved")
}

func (h *Handler) slurmShowNode(c *gin.Context) {
	details, err := h.scheduler.GetNodeDetails(c.Param("clusterName"), c.Param("nodeName"))
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	record := slurmNodeRecord(c.Param("clusterName"), details)
	util.Success(c, slurmProjectOne(record, slurmFieldsQuery(c)), "scontrol show node retrieved")
}

func (h *Handler) slurmShowDaemons(c *gin.Context) {
	records := h.slurmDaemonRecords()
	if daemonFilter := firstQuery(c, "daemon", "name", "type"); daemonFilter != "" {
		filtered := make([]map[string]interface{}, 0, len(records))
		for _, record := range records {
			if containsCSVFold(daemonFilter, fmt.Sprint(record["daemon"])) || containsCSVFold(daemonFilter, fmt.Sprint(record["service"])) {
				filtered = append(filtered, record)
			}
		}
		records = filtered
	}
	if clusterFilter := firstQuery(c, "cluster", "partition"); clusterFilter != "" {
		filtered := make([]map[string]interface{}, 0, len(records))
		for _, record := range records {
			if containsCSVFold(clusterFilter, fmt.Sprint(record["cluster"])) {
				filtered = append(filtered, record)
			}
		}
		records = filtered
	}
	if nodeFilter := firstQuery(c, "node", "nodelist"); nodeFilter != "" {
		filtered := make([]map[string]interface{}, 0, len(records))
		for _, record := range records {
			if containsCSVFold(nodeFilter, fmt.Sprint(record["node"])) {
				filtered = append(filtered, record)
			}
		}
		records = filtered
	}
	if statusFilter := firstQuery(c, "status", "state", "states"); statusFilter != "" {
		filtered := make([]map[string]interface{}, 0, len(records))
		for _, record := range records {
			if containsCSVFold(statusFilter, fmt.Sprint(record["status"])) || containsCSVFold(statusFilter, fmt.Sprint(record["state"])) {
				filtered = append(filtered, record)
			}
		}
		records = filtered
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "scontrol show daemons retrieved")
}

func (h *Handler) slurmScontrolPing(c *gin.Context) {
	daemons := h.slurmDaemonRecords()
	controllers := make([]map[string]interface{}, 0)
	downDaemons := make([]string, 0)
	responding := true
	for _, daemon := range daemons {
		if fmt.Sprint(daemon["daemon"]) == "slurmd" {
			continue
		}
		controllers = append(controllers, daemon)
		if ok, _ := daemon["responding"].(bool); !ok {
			responding = false
			downDaemons = append(downDaemons, fmt.Sprint(daemon["daemon_id"]))
		}
	}
	status := "UP"
	if !responding {
		status = "DOWN"
	}
	record := map[string]interface{}{
		"generated_at":     time.Now(),
		"responding":       responding,
		"status":           status,
		"mode":             "primary",
		"primary":          "csoj-admin-api",
		"controller_count": len(controllers),
		"daemon_count":     len(daemons),
		"cluster_count":    len(h.slurmClusterRecords()),
		"controllers":      controllers,
		"down_daemons":     downDaemons,
	}
	util.Success(c, slurmProjectOne(record, slurmFieldsQuery(c)), "scontrol ping retrieved")
}

func (h *Handler) slurmShowPartitions(c *gin.Context) {
	partitions := h.scheduler.ListPartitions(firstQuery(c, "partition", "name"))
	records := make([]map[string]interface{}, 0, len(partitions))
	for _, partition := range partitions {
		records = append(records, slurmPartitionRecord(partition))
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "scontrol show partitions retrieved")
}

func (h *Handler) slurmShowConfig(c *gin.Context) {
	configSnapshot := h.scheduler.GetSchedulerConfigSnapshot()
	partitions := h.scheduler.ListPartitions("")
	partitionRecords := make([]map[string]interface{}, 0, len(partitions))
	for _, partition := range partitions {
		partitionRecords = append(partitionRecords, slurmPartitionRecord(partition))
	}
	accounts := make([]map[string]interface{}, 0, len(configSnapshot.Accounts))
	for _, account := range configSnapshot.Accounts {
		accounts = append(accounts, slurmAccountRecord(account))
	}
	qosItems := make([]map[string]interface{}, 0, len(configSnapshot.QOS))
	for _, qos := range configSnapshot.QOS {
		qosItems = append(qosItems, slurmQOSRecord(qos))
	}
	reservations := make([]map[string]interface{}, 0, len(configSnapshot.Reservations))
	for _, reservation := range configSnapshot.Reservations {
		reservations = append(reservations, slurmReservationRecord(reservation))
	}

	record := map[string]interface{}{
		"queue_size":       configSnapshot.QueueSize,
		"backfill":         configSnapshot.Backfill,
		"priority_weights": configSnapshot.PriorityWeights,
		"billing_weights":  configSnapshot.BillingWeights,
		"fairshare_decay":  configSnapshot.FairshareDecay,
		"partitions":       partitionRecords,
		"licenses":         slurmLicenseRecords(h.scheduler.GetLicenseStatus()),
		"accounts":         accounts,
		"qos":              qosItems,
		"reservations":     reservations,
	}
	util.Success(c, slurmProjectOne(record, slurmFieldsQuery(c)), "scontrol show config retrieved")
}

func (h *Handler) slurmShowLicenses(c *gin.Context) {
	statuses := h.scheduler.GetLicenseStatus()
	records := make([]map[string]interface{}, 0, len(statuses))
	for _, status := range statuses {
		if license := c.Query("license"); license != "" && !strings.EqualFold(license, status.Name) {
			continue
		}
		records = append(records, map[string]interface{}{
			"license":   status.Name,
			"total":     status.Total,
			"used":      status.Used,
			"available": status.Available,
			"owners":    status.Owners,
		})
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "scontrol show licenses retrieved")
}

func (h *Handler) slurmShowReservations(c *gin.Context) {
	reservations := h.scheduler.ListReservations(firstQuery(c, "reservation", "name"))
	records := make([]map[string]interface{}, 0, len(reservations))
	for _, reservation := range reservations {
		records = append(records, slurmReservationRecord(reservation))
	}
	util.Success(c, slurmProjectFields(records, slurmFieldsQuery(c)), "scontrol show reservations retrieved")
}

func (h *Handler) slurmUpsertReservation(c *gin.Context) {
	var req slurmReservationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	reservation, err := h.scheduler.UpsertReservation(req.Config(c.Param("name")))
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	util.Success(c, slurmProjectOne(slurmReservationRecord(reservation), slurmFieldsQuery(c)), "reservation saved")
}

func (h *Handler) slurmDeleteReservation(c *gin.Context) {
	if err := h.scheduler.DeleteReservation(c.Param("name")); err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	util.Success(c, gin.H{"reservation": c.Param("name"), "deleted": true}, "reservation deleted")
}

func (h *Handler) slurmUpdateNode(c *gin.Context) {
	var req struct {
		State  string `json:"state"`
		Reason string `json:"reason"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	state := normalizeSlurmNodeState(req.State)
	if state == "" {
		util.Error(c, http.StatusBadRequest, "state is required")
		return
	}
	if err := h.scheduler.SetNodeState(c.Param("clusterName"), c.Param("nodeName"), state, req.Reason); err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	h.slurmShowNode(c)
}

func (h *Handler) slurmUpdatePartition(c *gin.Context) {
	var req slurmPartitionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	partition, err := h.scheduler.UpdatePartition(c.Param("name"), req.Config(c.Param("name")))
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	util.Success(c, slurmProjectOne(slurmPartitionRecord(partition), slurmFieldsQuery(c)), "scontrol update partition applied")
}

func (h *Handler) slurmSuspendJob(c *gin.Context) {
	h.slurmSetJobSuspended(c, true)
}

func (h *Handler) slurmResumeJob(c *gin.Context) {
	h.slurmSetJobSuspended(c, false)
}

func (h *Handler) slurmSignalJob(c *gin.Context) {
	sub, err := h.getSlurmJobBySelector(c.Param("id"))
	if err != nil {
		util.Error(c, http.StatusNotFound, "job not found")
		return
	}

	var req struct {
		Signal string `json:"signal"`
	}
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&req); err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
	}
	signalName := firstNonEmpty(req.Signal, c.Query("signal"))
	normalized, err := h.signalSlurmSubmission(sub, signalName)
	if err != nil {
		writeAdminStatusError(c, err)
		return
	}

	sub.PopulateSlurmState()
	response := slurmJobRecord(sub)
	response["signal"] = normalized
	util.Success(c, slurmProjectOne(response, slurmFieldsQuery(c)), "job signaled")
}

func (h *Handler) signalSlurmSubmission(sub *models.Submission, signalName string) (string, error) {
	if sub == nil {
		return "", newAdminStatusError(http.StatusNotFound, "job not found")
	}
	if sub.Status != models.StatusRunning && sub.Status != models.StatusSuspended {
		return "", newAdminStatusError(http.StatusBadRequest, "only running or suspended jobs can be signaled")
	}
	if err := judger.SignalRuntimeContainers(h.cfg, sub.Cluster, sub.Node, sub.Containers, signalName); err != nil {
		return "", newAdminStatusError(http.StatusInternalServerError, err)
	}

	normalized := judger.NormalizeSignal(signalName)
	record := database.AccountingFromSubmission(sub, database.AccountEventSignaled)
	record.Message = normalized
	if err := database.RecordAccounting(h.db, record); err != nil {
		zap.S().Warnf("failed to record signal event for submission %s: %v", sub.ID, err)
	}
	return normalized, nil
}

func (h *Handler) slurmSetJobSuspended(c *gin.Context, suspended bool) {
	sub, err := h.getSlurmJobBySelector(c.Param("id"))
	if err != nil {
		util.Error(c, http.StatusNotFound, "job not found")
		return
	}

	if suspended {
		if sub.Status != models.StatusRunning {
			util.Error(c, http.StatusBadRequest, "only running jobs can be suspended")
			return
		}
		if err := judger.SetRuntimeContainersPaused(h.cfg, sub.Cluster, sub.Node, sub.Containers, true); err != nil {
			util.Error(c, http.StatusInternalServerError, err)
			return
		}
		if err := h.db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&models.Submission{}).Where("id = ?", sub.ID).Updates(map[string]interface{}{
				"status": models.StatusSuspended,
				"reason": "Suspended",
			}).Error; err != nil {
				return err
			}
			return tx.Model(&models.Container{}).Where("submission_id = ? AND status = ?", sub.ID, models.StatusRunning).Update("status", models.StatusSuspended).Error
		}); err != nil {
			util.Error(c, http.StatusInternalServerError, err)
			return
		}
		sub.Status = models.StatusSuspended
		sub.Reason = "Suspended"
		if err := database.RecordAccounting(h.db, database.AccountingFromSubmission(sub, database.AccountEventSuspended)); err != nil {
			zap.S().Warnf("failed to record suspend event for submission %s: %v", sub.ID, err)
		}
		sub.PopulateSlurmState()
		util.Success(c, slurmProjectOne(slurmJobRecord(sub), slurmFieldsQuery(c)), "job suspended")
		return
	}

	if sub.Status != models.StatusSuspended {
		util.Error(c, http.StatusBadRequest, "only suspended jobs can be resumed")
		return
	}
	if err := judger.SetRuntimeContainersPaused(h.cfg, sub.Cluster, sub.Node, sub.Containers, false); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	if err := h.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.Submission{}).Where("id = ?", sub.ID).Updates(map[string]interface{}{
			"status": models.StatusRunning,
			"reason": "",
		}).Error; err != nil {
			return err
		}
		return tx.Model(&models.Container{}).Where("submission_id = ? AND status = ?", sub.ID, models.StatusSuspended).Update("status", models.StatusRunning).Error
	}); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	sub.Status = models.StatusRunning
	sub.Reason = ""
	if err := database.RecordAccounting(h.db, database.AccountingFromSubmission(sub, database.AccountEventResumed)); err != nil {
		zap.S().Warnf("failed to record resume event for submission %s: %v", sub.ID, err)
	}
	sub.PopulateSlurmState()
	util.Success(c, slurmProjectOne(slurmJobRecord(sub), slurmFieldsQuery(c)), "job resumed")
}

func (h *Handler) slurmUpdateJob(c *gin.Context) {
	sub, err := h.getSlurmJobBySelector(c.Param("id"))
	if err != nil {
		util.Error(c, http.StatusNotFound, "job not found")
		return
	}

	var req struct {
		Account      *string         `json:"account"`
		QOS          *string         `json:"qos"`
		JobName      *string         `json:"job_name"`
		Name         *string         `json:"name"`
		WorkDir      *string         `json:"work_dir"`
		Chdir        *string         `json:"chdir"`
		StdinPath    *string         `json:"stdin_path"`
		Input        *string         `json:"input"`
		StdoutPath   *string         `json:"stdout_path"`
		Output       *string         `json:"output"`
		StderrPath   *string         `json:"stderr_path"`
		ErrorPath    *string         `json:"error"`
		OpenMode     *string         `json:"open_mode"`
		Comment      *string         `json:"comment"`
		MailType     *string         `json:"mail_type"`
		MailUser     *string         `json:"mail_user"`
		Exclusive    *bool           `json:"exclusive"`
		Requeue      *bool           `json:"requeue"`
		Export       *string         `json:"export"`
		Environment  *models.JSONMap `json:"environment"`
		Priority     *int            `json:"priority"`
		Nice         *int            `json:"nice"`
		Hold         *bool           `json:"hold"`
		CPU          *int            `json:"cpus"`
		NTasks       *int            `json:"ntasks"`
		CPUsPerTask  *int            `json:"cpus_per_task"`
		Nodes        *int            `json:"nodes"`
		Memory       *slurmMemoryMB  `json:"memory"`
		Mem          *slurmMemoryMB  `json:"mem"`
		BeginTime    *slurmDateTime  `json:"begin_time"`
		Begin        *slurmDateTime  `json:"begin"`
		StartTime    *slurmDateTime  `json:"start_time"`
		Deadline     *slurmDateTime  `json:"deadline"`
		TimeLimit    *slurmTimeLimit `json:"time_limit"`
		Time         *slurmTimeLimit `json:"time"`
		Dependencies *string         `json:"dependencies"`
		Reservation  *string         `json:"reservation"`
		NodeList     *string         `json:"node_list"`
		Nodelist     *string         `json:"nodelist"`
		NodesList    *string         `json:"nodeslist"`
		Exclude      *string         `json:"exclude"`
		ExcludeNodes *string         `json:"exclude_nodes"`
		Constraint   *string         `json:"constraint"`
		GRES         *string         `json:"gres"`
		TRES         *string         `json:"tres"`
		Licenses     *string         `json:"licenses"`
		Reason       *string         `json:"reason"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	if req.TimeLimit == nil {
		req.TimeLimit = req.Time
	}
	if req.Memory == nil {
		req.Memory = req.Mem
	}
	if req.BeginTime == nil {
		req.BeginTime = firstSlurmDateTime(req.Begin, req.StartTime)
	}
	if req.NodeList == nil {
		if req.Nodelist != nil {
			req.NodeList = req.Nodelist
		} else if req.NodesList != nil {
			req.NodeList = req.NodesList
		}
	}
	if req.ExcludeNodes == nil {
		req.ExcludeNodes = req.Exclude
	}

	holdChanged := req.Hold != nil && sub.Hold != *req.Hold
	if req.Account != nil {
		sub.Account = *req.Account
	}
	if req.QOS != nil {
		sub.QOS = *req.QOS
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
	if req.Priority != nil {
		sub.Priority = *req.Priority
	}
	if req.Nice != nil {
		sub.Nice = *req.Nice
	}
	if req.Hold != nil {
		sub.Hold = *req.Hold
		if sub.Hold {
			sub.Reason = "JobHeld"
		} else if sub.Reason == "JobHeld" {
			sub.Reason = ""
		}
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
		if req.Memory.Int64() <= 0 {
			util.Error(c, http.StatusBadRequest, "memory must be positive")
			return
		}
		sub.Memory = req.Memory.Int64()
	}
	if req.BeginTime != nil {
		sub.BeginTime = req.BeginTime.Ptr()
	}
	if req.Deadline != nil {
		sub.Deadline = req.Deadline.Ptr()
	}
	if req.TimeLimit != nil {
		sub.TimeLimit = req.TimeLimit.Int()
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
	if req.Reason != nil {
		sub.Reason = *req.Reason
	}

	if err := database.UpdateSubmission(h.db, sub); err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	if holdChanged {
		event := database.AccountEventReleased
		if sub.Hold {
			event = database.AccountEventHeld
		}
		if err := database.RecordAccounting(h.db, database.AccountingFromSubmission(sub, event)); err != nil {
			zap.S().Warnf("failed to record slurm hold update event for submission %s: %v", sub.ID, err)
		}
	}

	sub.PopulateSlurmState()
	util.Success(c, slurmProjectOne(slurmJobRecord(sub), slurmFieldsQuery(c)), "scontrol update job applied")
}

func (h *Handler) slurmHoldJobs(c *gin.Context) {
	h.slurmSetHoldJobs(c, true)
}

func (h *Handler) slurmReleaseJobs(c *gin.Context) {
	h.slurmSetHoldJobs(c, false)
}

func (h *Handler) slurmSetHoldJobs(c *gin.Context, hold bool) {
	submissions, err := h.slurmJobsByPathSelector(c.Param("id"))
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			util.Error(c, http.StatusNotFound, "job not found")
			return
		}
		util.Error(c, http.StatusBadRequest, err)
		return
	}

	items := make([]map[string]interface{}, 0, len(submissions))
	changed := 0
	failed := 0
	for i := range submissions {
		sub := &submissions[i]
		if sub.Status != models.StatusQueued {
			failed++
			item := slurmJobActionRecord(sub)
			item["held"] = false
			item["released"] = false
			item["error"] = "only queued jobs can be held or released"
			items = append(items, item)
			continue
		}

		sub.Hold = hold
		if hold {
			sub.Reason = "JobHeld"
		} else if sub.Reason == "JobHeld" {
			sub.Reason = ""
		}
		if err := database.UpdateSubmission(h.db, sub); err != nil {
			failed++
			item := slurmJobActionRecord(sub)
			item["held"] = false
			item["released"] = false
			item["error"] = err.Error()
			items = append(items, item)
			continue
		}
		event := database.AccountEventReleased
		if hold {
			event = database.AccountEventHeld
		}
		if err := database.RecordAccounting(h.db, database.AccountingFromSubmission(sub, event)); err != nil {
			zap.S().Warnf("failed to record accounting hold event for submission %s: %v", sub.ID, err)
		}

		changed++
		item := slurmJobActionRecord(sub)
		item["held"] = hold
		item["released"] = !hold
		items = append(items, item)
	}

	response := gin.H{
		"items":   slurmProjectFields(items, slurmFieldsQuery(c)),
		"matched": len(items),
		"failed":  failed,
	}
	if hold {
		response["held"] = changed
	} else {
		response["released"] = changed
	}
	util.Success(c, response, "job hold state updated")
}

func (h *Handler) slurmRequeueJobs(c *gin.Context) {
	submissions, err := h.slurmJobsByPathSelector(c.Param("id"))
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			util.Error(c, http.StatusNotFound, "job not found")
			return
		}
		util.Error(c, http.StatusBadRequest, err)
		return
	}

	items := make([]map[string]interface{}, 0, len(submissions))
	requeued := 0
	failed := 0
	for i := range submissions {
		sub := &submissions[i]
		if submissionActiveForAdmin(sub.Status) {
			failed++
			item := slurmJobActionRecord(sub)
			item["requeued"] = false
			item["error"] = "running jobs must be interrupted before requeue"
			items = append(items, item)
			continue
		}

		h.appState.RLock()
		problem, ok := h.appState.Problems[sub.ProblemID]
		h.appState.RUnlock()
		if !ok {
			failed++
			item := slurmJobActionRecord(sub)
			item["requeued"] = false
			item["error"] = "problem definition not found for requeue"
			items = append(items, item)
			continue
		}

		if err := h.requeueSlurmSubmission(sub, problem); err != nil {
			failed++
			item := slurmJobActionRecord(sub)
			item["requeued"] = false
			item["error"] = err.Error()
			items = append(items, item)
			continue
		}
		requeued++
		item := slurmJobActionRecord(sub)
		item["requeued"] = true
		items = append(items, item)
	}

	util.Success(c, gin.H{
		"items":    slurmProjectFields(items, slurmFieldsQuery(c)),
		"matched":  len(items),
		"requeued": requeued,
		"failed":   failed,
	}, "jobs requeued")
}

func (h *Handler) slurmCancelJobs(c *gin.Context) {
	submissions, err := h.slurmJobsByPathSelector(c.Param("id"))
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			util.Error(c, http.StatusNotFound, "job not found")
			return
		}
		util.Error(c, http.StatusBadRequest, err)
		return
	}

	items := make([]map[string]interface{}, 0, len(submissions))
	cancelled := 0
	failed := 0
	for i := range submissions {
		sub := &submissions[i]
		item := slurmJobActionRecord(sub)
		item["cancelled"] = false
		message, err := h.interruptSubmissionByID(sub.ID)
		if err != nil {
			failed++
			item["error"] = err.Error()
		} else {
			cancelled++
			item["cancelled"] = true
			item["state"] = models.SlurmStateCancelled
			item["native_status"] = models.StatusFailed
			item["reason"] = "Cancelled"
			item["message"] = message
		}
		items = append(items, item)
	}

	util.Success(c, gin.H{
		"items":     slurmProjectFields(items, slurmFieldsQuery(c)),
		"matched":   len(items),
		"cancelled": cancelled,
		"failed":    failed,
	}, "jobs cancelled")
}

func (h *Handler) requeueSlurmSubmission(sub *models.Submission, problem *judger.Problem) error {
	if sub == nil || problem == nil {
		return fmt.Errorf("submission and problem are required")
	}
	if err := h.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("submission_id = ?", sub.ID).Delete(&models.Container{}).Error; err != nil {
			return err
		}
		return tx.Model(&models.Submission{}).Where("id = ?", sub.ID).Updates(map[string]interface{}{
			"status":               models.StatusQueued,
			"current_step":         0,
			"node":                 "",
			"allocated_cores":      "",
			"allocated_node_cores": "",
			"score":                0,
			"performance":          0,
			"info":                 models.JSONMap{},
			"reason":               "",
		}).Error
	}); err != nil {
		return fmt.Errorf("failed to requeue job: %w", err)
	}

	sub.Status = models.StatusQueued
	sub.CurrentStep = 0
	sub.Node = ""
	sub.AllocatedCores = ""
	sub.AllocatedNodeCores = ""
	sub.Score = 0
	sub.Performance = 0
	sub.Info = models.JSONMap{}
	sub.Reason = ""
	if err := database.RecordAccounting(h.db, database.AccountingFromSubmission(sub, database.AccountEventRequeued)); err != nil {
		zap.S().Warnf("failed to record accounting requeue event for submission %s: %v", sub.ID, err)
	}
	h.scheduler.Submit(sub, problem)
	return nil
}

func (h *Handler) slurmJobsByPathSelector(selector string) ([]models.Submission, error) {
	jobSelectors, err := parseSlurmJobSelectors(selector)
	if err != nil {
		return nil, err
	}
	if !jobSelectors.Has() {
		return nil, gorm.ErrRecordNotFound
	}
	query := applySlurmArrayWideJobSelectorQuery(h.db.Model(&models.Submission{}), "id", jobSelectors)
	var submissions []models.Submission
	if err := query.Order("array_job_id asc, array_task_id asc, created_at asc").Find(&submissions).Error; err != nil {
		return nil, err
	}
	if len(submissions) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	return submissions, nil
}

func slurmJobActionRecord(sub *models.Submission) map[string]interface{} {
	if sub == nil {
		return map[string]interface{}{}
	}
	sub.PopulateSlurmState()
	return slurmJobRecord(sub)
}

func (h *Handler) slurmScancel(c *gin.Context) {
	var req struct {
		JobID       string   `json:"job_id"`
		JobIDs      []string `json:"job_ids"`
		User        string   `json:"user"`
		UserID      string   `json:"user_id"`
		Partition   string   `json:"partition"`
		Cluster     string   `json:"cluster"`
		JobName     string   `json:"job_name"`
		Name        string   `json:"name"`
		ArrayJobID  string   `json:"array_job_id"`
		ArrayTaskID string   `json:"array_task_id"`
		State       string   `json:"state"`
		Account     string   `json:"account"`
		QOS         string   `json:"qos"`
		Signal      string   `json:"signal"`
	}
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&req); err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
	}

	jobSelectors, err := parseSlurmJobSelectors(req.JobID, strings.Join(req.JobIDs, ","), firstQuery(c, "job_id", "id"))
	if err != nil {
		util.Error(c, http.StatusBadRequest, err)
		return
	}
	userID := firstNonEmpty(req.UserID, req.User, firstQuery(c, "user_id", "user"))
	partition := firstNonEmpty(req.Partition, req.Cluster, firstQuery(c, "partition", "cluster"))
	jobName := firstNonEmpty(req.JobName, req.Name, firstQuery(c, "job_name", "name"))
	arrayJobID := firstNonEmpty(req.ArrayJobID, c.Query("array_job_id"))
	arrayTaskID := firstNonEmpty(req.ArrayTaskID, firstQuery(c, "array_task_id", "array_task"))
	stateFilter := firstNonEmpty(req.State, slurmStateQuery(c))
	account := firstNonEmpty(req.Account, c.Query("account"))
	qos := firstNonEmpty(req.QOS, c.Query("qos"))
	signalName := firstNonEmpty(req.Signal, firstQuery(c, "signal", "s"))
	if !jobSelectors.Has() && userID == "" && partition == "" && jobName == "" && arrayJobID == "" && arrayTaskID == "" && stateFilter == "" && account == "" && qos == "" {
		util.Error(c, http.StatusBadRequest, "at least one scancel selector is required")
		return
	}

	query := h.db.Preload("Containers").Model(&models.Submission{}).Where("status IN ?", []models.Status{models.StatusQueued, models.StatusRunning, models.StatusSuspended})
	if jobSelectors.Has() {
		query = applySlurmJobSelectorQuery(query, "id", jobSelectors)
	}
	if userID != "" {
		query = query.Where("user_id IN ?", slurmCSVValues(userID))
	}
	if partition != "" {
		query = query.Where("cluster IN ?", slurmCSVValues(partition))
	}
	if jobName != "" {
		jobNames := slurmCSVValues(jobName)
		query = query.Where("job_name IN ? OR (job_name = '' AND problem_id IN ?)", jobNames, jobNames)
	}
	if arrayJobID != "" {
		query = query.Where("array_job_id IN ?", slurmCSVValues(arrayJobID))
	}
	if arrayTaskID != "" {
		taskIDs, err := slurmCSVInts(arrayTaskID)
		if err != nil {
			util.Error(c, http.StatusBadRequest, err)
			return
		}
		query = query.Where("array_task_id IN ?", taskIDs)
	}
	if account != "" {
		query = query.Where("account IN ?", slurmCSVValues(account))
	}
	if qos != "" {
		query = query.Where("qos IN ?", slurmCSVValues(qos))
	}

	var submissions []models.Submission
	if err := query.Order("created_at asc").Find(&submissions).Error; err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	items := make([]map[string]interface{}, 0, len(submissions))
	cancelled := 0
	signaled := 0
	failed := 0
	for i := range submissions {
		sub := &submissions[i]
		slurmState, slurmReason := models.DeriveSlurmJobState(sub.Status, sub.Hold, sub.Reason)
		if !slurmStateFilterMatches(stateFilter, slurmState, string(sub.Status)) {
			continue
		}
		item := map[string]interface{}{
			"job_id":        sub.ID,
			"job_id_raw":    sub.ID,
			"array_job_id":  sub.ArrayJobID,
			"array_task_id": sub.ArrayTaskID,
			"name":          slurmSubmissionJobName(sub),
			"job_name":      slurmSubmissionJobName(sub),
			"problem_id":    sub.ProblemID,
			"user_id":       sub.UserID,
			"partition":     sub.Cluster,
			"state":         slurmState,
			"native_status": sub.Status,
			"reason":        slurmReason,
			"cancelled":     false,
			"signaled":      false,
		}
		if signalName != "" {
			normalized, err := h.signalSlurmSubmission(sub, signalName)
			if err != nil {
				failed++
				item["error"] = err.Error()
			} else {
				signaled++
				item["signaled"] = true
				item["signal"] = normalized
			}
			items = append(items, item)
			continue
		}
		message, err := h.interruptSubmissionByID(sub.ID)
		if err != nil {
			failed++
			item["error"] = err.Error()
		} else {
			cancelled++
			item["cancelled"] = true
			item["state"] = models.SlurmStateCancelled
			item["native_status"] = models.StatusFailed
			item["reason"] = "Cancelled"
			item["message"] = message
		}
		items = append(items, item)
	}

	util.Success(c, gin.H{
		"items":     slurmProjectFields(items, slurmFieldsQuery(c)),
		"matched":   len(items),
		"cancelled": cancelled,
		"signaled":  signaled,
		"failed":    failed,
	}, "scancel applied")
}

func matchesSlurmQueueFilters(entry judger.QueueEntry, c *gin.Context) (bool, error) {
	if jobID := firstQuery(c, "job_id", "id"); jobID != "" {
		jobSelectors, err := parseSlurmJobSelectors(jobID)
		if err != nil {
			return false, err
		}
		if jobSelectors.Has() && !jobSelectors.Matches(entry.ID, entry.ArrayJobID, entry.ArrayTaskID) {
			return false, nil
		}
	}
	if arrayJobID := c.Query("array_job_id"); arrayJobID != "" && !containsCSVFold(arrayJobID, entry.ArrayJobID) {
		return false, nil
	}
	if arrayTaskID := firstQuery(c, "array_task_id", "array_task"); arrayTaskID != "" && !containsCSVFold(arrayTaskID, strconv.Itoa(entry.ArrayTaskID)) {
		return false, nil
	}
	if partition := firstQuery(c, "partition", "cluster"); partition != "" && !containsCSVFold(partition, entry.Cluster) {
		return false, nil
	}
	if !slurmStateFilterMatches(slurmStateQuery(c), entry.SlurmState, string(entry.Status)) {
		return false, nil
	}
	if user := firstQuery(c, "user", "user_id"); user != "" && !containsCSVFold(user, entry.UserID) {
		return false, nil
	}
	if jobName := firstQuery(c, "job_name", "name"); jobName != "" && !containsCSVFold(jobName, slurmQueueJobName(entry)) {
		return false, nil
	}
	if account := c.Query("account"); account != "" && !containsCSVFold(account, entry.Account) {
		return false, nil
	}
	if qos := c.Query("qos"); qos != "" && !containsCSVFold(qos, entry.QOS) {
		return false, nil
	}
	return true, nil
}

func slurmQueueRecord(entry judger.QueueEntry) map[string]interface{} {
	nodeList := entry.Node
	if nodeList == "" {
		nodeList = entry.Reason
	}
	return map[string]interface{}{
		"job_id":               entry.ID,
		"job_id_raw":           entry.ID,
		"array_job_id":         entry.ArrayJobID,
		"array_task_id":        entry.ArrayTaskID,
		"partition":            entry.Cluster,
		"name":                 slurmQueueJobName(entry),
		"job_name":             slurmQueueJobName(entry),
		"problem_id":           entry.Problem,
		"user_id":              entry.UserID,
		"state":                entry.SlurmState,
		"native_status":        entry.Status,
		"reason":               entry.SlurmReason,
		"nodelist":             nodeList,
		"node":                 entry.Node,
		"allocated_node_cores": entry.AllocatedNodeCores,
		"qos":                  entry.QOS,
		"account":              entry.Account,
		"cpus":                 entry.CPU,
		"ntasks":               entry.NTasks,
		"cpus_per_task":        entry.CPUsPerTask,
		"nodes":                slurmJobNodeCount(entry.Nodes),
		"memory":               entry.Memory,
		"tres":                 entry.TRES,
		"licenses":             entry.Licenses,
		"requested_nodelist":   entry.NodeList,
		"exclude_nodes":        entry.ExcludeNodes,
		"billing_units":        entry.BillingUnits,
		"priority":             entry.Priority,
		"queue_position":       entry.Position,
		"submit_time":          entry.Created,
	}
}

func slurmAccountingRecord(record models.AccountingRecord) map[string]interface{} {
	state, reason := models.DeriveSlurmJobState(record.State, false, record.Reason)
	if record.Event == database.AccountEventInterrupted {
		state = models.SlurmStateCancelled
		reason = "Cancelled"
	}
	if record.Event == database.AccountEventPreempted {
		state = models.SlurmStatePreempted
		reason = "Preempted"
	}
	return map[string]interface{}{
		"job_id":          record.SubmissionID,
		"job_id_raw":      record.SubmissionID,
		"container_id":    record.ContainerID,
		"array_job_id":    record.ArrayJobID,
		"array_task_id":   record.ArrayTaskID,
		"user_id":         record.UserID,
		"partition":       record.Cluster,
		"node":            record.Node,
		"account":         record.Account,
		"qos":             record.QOS,
		"job_name":        slurmAccountingJobName(record),
		"problem_id":      record.ProblemID,
		"event":           record.Event,
		"state":           state,
		"native_state":    record.State,
		"reason":          reason,
		"step_name":       record.StepName,
		"exit_code":       record.ExitCode,
		"alloc_cpus":      record.CPU,
		"alloc_mem":       record.Memory,
		"tres":            record.TRES,
		"billing_units":   record.BillingUnits,
		"score":           record.Score,
		"performance":     record.Performance,
		"message":         record.Message,
		"accounting_time": record.CreatedAt,
	}
}

func slurmPriorityRecord(item judger.PriorityBreakdown) map[string]interface{} {
	return map[string]interface{}{
		"job_id":             item.JobID,
		"job_id_raw":         item.JobID,
		"array_job_id":       item.ArrayJobID,
		"array_task_id":      item.ArrayTaskID,
		"user_id":            item.UserID,
		"partition":          item.Partition,
		"account":            item.Account,
		"qos":                item.QOS,
		"state":              item.SlurmState,
		"native_status":      item.Status,
		"priority":           item.Priority,
		"manual_priority":    item.ManualPriority,
		"partition_priority": item.PartitionPriority,
		"qos_priority":       item.QOSPriority,
		"fairshare_priority": item.FairsharePriority,
		"fairshare_penalty":  item.FairsharePenalty,
		"age_priority":       item.AgePriority,
		"job_size_priority":  item.JobSizePriority,
		"nice_penalty":       item.NicePenalty,
	}
}

func slurmFairshareRecord(item judger.FairshareRecord) map[string]interface{} {
	return map[string]interface{}{
		"account":           item.Account,
		"parent_account":    item.ParentAccount,
		"raw_shares":        item.RawShares,
		"normalized_shares": item.NormalizedShares,
		"raw_usage":         item.RawUsage,
		"effective_usage":   item.EffectiveUsage,
		"usage_penalty":     item.UsagePenalty,
		"running_jobs":      item.RunningJobs,
		"submitted_jobs":    item.SubmittedJobs,
	}
}

func slurmDiagnosticPartitions(states map[string]judger.ClusterState) ([]map[string]interface{}, map[string]int64) {
	names := make([]string, 0, len(states))
	for name := range states {
		names = append(names, name)
	}
	sort.Strings(names)

	totals := map[string]int64{
		"nodes":            0,
		"total_cpus":       0,
		"allocated_cpus":   0,
		"idle_cpus":        0,
		"total_memory":     0,
		"allocated_memory": 0,
		"idle_memory":      0,
	}
	records := make([]map[string]interface{}, 0, len(names))
	for _, name := range names {
		cluster := states[name]
		nodeNames := make([]string, 0, len(cluster.Nodes))
		for nodeName := range cluster.Nodes {
			nodeNames = append(nodeNames, nodeName)
		}
		sort.Strings(nodeNames)

		totalCPU := int64(0)
		allocatedCPU := int64(0)
		totalMemory := int64(0)
		allocatedMemory := int64(0)
		downNodes := 0
		drainedNodes := 0
		for _, nodeName := range nodeNames {
			node := cluster.Nodes[nodeName]
			totalCPU += int64(node.CPU)
			totalMemory += node.Memory
			allocatedMemory += node.UsedMemory
			for _, used := range node.UsedCores {
				if used {
					allocatedCPU++
				}
			}
			state := strings.ToLower(node.State)
			if state == "down" || state == "inactive" {
				downNodes++
			}
			if node.IsPaused || state == "drain" || state == "drained" {
				drainedNodes++
			}
		}
		idleCPU := totalCPU - allocatedCPU
		if idleCPU < 0 {
			idleCPU = 0
		}
		idleMemory := totalMemory - allocatedMemory
		if idleMemory < 0 {
			idleMemory = 0
		}
		totals["nodes"] += int64(len(nodeNames))
		totals["total_cpus"] += totalCPU
		totals["allocated_cpus"] += allocatedCPU
		totals["idle_cpus"] += idleCPU
		totals["total_memory"] += totalMemory
		totals["allocated_memory"] += allocatedMemory
		totals["idle_memory"] += idleMemory
		records = append(records, map[string]interface{}{
			"partition":        name,
			"node_count":       len(nodeNames),
			"nodes":            nodeNames,
			"down_nodes":       downNodes,
			"drained_nodes":    drainedNodes,
			"total_cpus":       totalCPU,
			"allocated_cpus":   allocatedCPU,
			"idle_cpus":        idleCPU,
			"total_memory":     totalMemory,
			"allocated_memory": allocatedMemory,
			"idle_memory":      idleMemory,
			"max_time":         cluster.MaxTime,
			"priority_tier":    cluster.PriorityTier,
		})
	}
	return records, totals
}

func (h *Handler) slurmClusterRecords() []map[string]interface{} {
	partitions := h.scheduler.ListPartitions("")
	states := h.scheduler.GetClusterStates()
	queueLengths := h.scheduler.GetQueueLengths()
	configSnapshot := h.scheduler.GetSchedulerConfigSnapshot()
	licenseRecords := slurmLicenseRecords(h.scheduler.GetLicenseStatus())
	apiListen := ""
	if h.cfg != nil {
		apiListen = firstNonEmpty(h.cfg.Admin.Listen, h.cfg.Listen)
	}

	records := make([]map[string]interface{}, 0, len(partitions))
	for _, partition := range partitions {
		state := strings.ToLower(strings.TrimSpace(partition.State))
		if state == "" {
			state = "up"
		}

		liveState := states[partition.Name]
		nodeConfigs := make(map[string]config.Node, len(partition.Nodes))
		nodeNameSet := make(map[string]bool)
		for _, node := range partition.Nodes {
			nodeConfigs[node.Name] = node
			nodeNameSet[node.Name] = true
		}
		for nodeName := range liveState.Nodes {
			nodeNameSet[nodeName] = true
		}
		nodeNames := sortedSlurmStringSet(nodeNameSet)

		totalCPU := 0
		allocatedCPU := 0
		totalMemory := int64(0)
		allocatedMemory := int64(0)
		downNodes := 0
		drainedNodes := 0
		featureSet := make(map[string]bool)
		runtimeSet := make(map[string]bool)
		tresCounts := map[string]int64{
			"cpu":  0,
			"mem":  0,
			"node": int64(len(nodeNames)),
		}

		for _, nodeName := range nodeNames {
			node := nodeConfigs[nodeName]
			liveNode := liveState.Nodes[nodeName]
			usedMemory := int64(0)
			usedCPU := 0
			paused := false
			if liveNode != nil && liveNode.Node != nil {
				node = *liveNode.Node
				usedMemory = liveNode.UsedMemory
				usedCPU = slurmUsedCPU(liveNode.UsedCores)
				paused = liveNode.IsPaused
			}
			totalCPU += node.CPU
			allocatedCPU += usedCPU
			totalMemory += node.Memory
			allocatedMemory += usedMemory
			for _, feature := range node.Features {
				if strings.TrimSpace(feature) != "" {
					featureSet[feature] = true
				}
			}
			for _, gres := range node.GRES {
				name, count, ok := slurmTRESResourceCount(gres)
				if !ok {
					continue
				}
				typ, resourceName, tres := slurmCanonicalTRESName(name, nil)
				if typ == "" {
					resourceName = strings.TrimPrefix(name, "gres/")
					tres = "gres/" + resourceName
				}
				if resourceName == "" {
					continue
				}
				tresCounts[tres] += count
			}
			runtimeSet[judger.NodeRuntimeName(node)] = true
			nodeState := slurmEffectiveNodeState(node.State, paused, node.CPU, usedCPU)
			switch nodeState {
			case "down", "inactive":
				downNodes++
			case "drain", "drained":
				drainedNodes++
			}
		}
		tresCounts["cpu"] = int64(totalCPU)
		tresCounts["mem"] = totalMemory

		idleCPU := totalCPU - allocatedCPU
		if idleCPU < 0 {
			idleCPU = 0
		}
		idleMemory := totalMemory - allocatedMemory
		if idleMemory < 0 {
			idleMemory = 0
		}
		records = append(records, map[string]interface{}{
			"cluster":          partition.Name,
			"name":             partition.Name,
			"partition":        partition.Name,
			"state":            strings.ToUpper(state),
			"native_state":     state,
			"classification":   "",
			"control_host":     "csoj-admin-api",
			"control_addr":     apiListen,
			"rpc":              "http-json",
			"node_count":       len(nodeNames),
			"nodes":            nodeNames,
			"node_names":       nodeNames,
			"down_nodes":       downNodes,
			"drained_nodes":    drainedNodes,
			"total_cpus":       totalCPU,
			"allocated_cpus":   allocatedCPU,
			"idle_cpus":        idleCPU,
			"total_memory":     totalMemory,
			"allocated_memory": allocatedMemory,
			"idle_memory":      idleMemory,
			"tres":             slurmClusterTRESString(tresCounts),
			"features":         sortedSlurmStringSet(featureSet),
			"runtimes":         sortedSlurmStringSet(runtimeSet),
			"queue_length":     queueLengths[partition.Name],
			"priority_tier":    partition.PriorityTier,
			"max_time":         partition.MaxTime,
			"max_jobs":         partition.MaxJobs,
			"account_count":    len(configSnapshot.Accounts),
			"qos_count":        len(configSnapshot.QOS),
			"license_count":    len(licenseRecords),
			"licenses":         licenseRecords,
		})
	}
	return records
}

func slurmClusterTRESString(counts map[string]int64) string {
	keys := make([]string, 0, len(counts))
	for key, count := range counts {
		if strings.TrimSpace(key) == "" || count == 0 {
			continue
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		leftType, leftName, _ := slurmCanonicalTRESName(keys[i], nil)
		rightType, rightName, _ := slurmCanonicalTRESName(keys[j], nil)
		if leftType == "" {
			leftType = keys[i]
			leftName = keys[i]
		}
		if rightType == "" {
			rightType = keys[j]
			rightName = keys[j]
		}
		if leftType == rightType {
			return leftName < rightName
		}
		return slurmTRESTypeRank(leftType) < slurmTRESTypeRank(rightType)
	})
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == "mem" {
			parts = append(parts, fmt.Sprintf("mem=%dM", counts[key]))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%d", key, counts[key]))
	}
	return strings.Join(parts, ",")
}

func (h *Handler) slurmDaemonRecords() []map[string]interface{} {
	generatedAt := time.Now()
	apiListen := ""
	if h.cfg != nil {
		apiListen = firstNonEmpty(h.cfg.Admin.Listen, h.cfg.Listen)
	}
	records := []map[string]interface{}{
		{
			"daemon_id":    "slurmrestd/csoj-admin-api",
			"daemon":       "slurmrestd",
			"service":      "csoj-admin-api",
			"role":         "api",
			"cluster":      "",
			"partition":    "",
			"node":         "",
			"state":        "UP",
			"status":       "UP",
			"responding":   true,
			"should_run":   true,
			"listen":       apiListen,
			"message":      "admin API handler active",
			"generated_at": generatedAt,
		},
		{
			"daemon_id":     "slurmctld/csoj-scheduler",
			"daemon":        "slurmctld",
			"service":       "csoj-scheduler",
			"role":          "controller",
			"cluster":       "",
			"partition":     "",
			"node":          "",
			"state":         "UP",
			"status":        "UP",
			"responding":    h.scheduler != nil,
			"should_run":    true,
			"queue_lengths": h.scheduler.GetQueueLengths(),
			"message":       "scheduler controller active",
			"generated_at":  generatedAt,
		},
	}

	dbResponding, dbMessage := h.slurmDatabaseStatus()
	dbStatus := "UP"
	if !dbResponding {
		dbStatus = "DOWN"
	}
	records = append(records, map[string]interface{}{
		"daemon_id":    "slurmdbd/database",
		"daemon":       "slurmdbd",
		"service":      "database",
		"role":         "accounting_storage",
		"cluster":      "",
		"partition":    "",
		"node":         "",
		"state":        dbStatus,
		"status":       dbStatus,
		"responding":   dbResponding,
		"should_run":   true,
		"message":      dbMessage,
		"generated_at": generatedAt,
	})

	states := h.scheduler.GetClusterStates()
	clusterNames := make([]string, 0, len(states))
	for clusterName := range states {
		clusterNames = append(clusterNames, clusterName)
	}
	sort.Strings(clusterNames)
	for _, clusterName := range clusterNames {
		cluster := states[clusterName]
		nodeNames := make([]string, 0, len(cluster.Nodes))
		for nodeName := range cluster.Nodes {
			nodeNames = append(nodeNames, nodeName)
		}
		sort.Strings(nodeNames)
		for _, nodeName := range nodeNames {
			node := cluster.Nodes[nodeName]
			state := strings.ToUpper(slurmNodeStateFromState(node))
			status := "UP"
			responding := true
			if state == "DOWN" || state == "INACTIVE" {
				status = "DOWN"
				responding = false
			} else if state == "DRAIN" || state == "DRAINED" {
				status = "DRAIN"
			}
			message := "node runtime available"
			if node.Reason != "" {
				message = node.Reason
			}
			records = append(records, map[string]interface{}{
				"daemon_id":    "slurmd/" + clusterName + "/" + nodeName,
				"daemon":       "slurmd",
				"service":      "node-runtime",
				"role":         "node",
				"cluster":      clusterName,
				"partition":    clusterName,
				"node":         nodeName,
				"state":        state,
				"status":       status,
				"responding":   responding,
				"should_run":   true,
				"runtime":      judger.NodeRuntimeName(*node.Node),
				"message":      message,
				"generated_at": generatedAt,
			})
		}
	}
	return records
}

func (h *Handler) slurmDatabaseStatus() (bool, string) {
	if h.db == nil {
		return false, "database handle unavailable"
	}
	sqlDB, err := h.db.DB()
	if err != nil {
		return false, err.Error()
	}
	if err := sqlDB.Ping(); err != nil {
		return false, err.Error()
	}
	return true, "database ping ok"
}

func slurmLicenseRecords(statuses []judger.LicenseStatus) []map[string]interface{} {
	records := make([]map[string]interface{}, 0, len(statuses))
	for _, status := range statuses {
		records = append(records, map[string]interface{}{
			"license":   status.Name,
			"total":     status.Total,
			"used":      status.Used,
			"available": status.Available,
			"owners":    status.Owners,
		})
	}
	return records
}

func slurmCountByField(db *gorm.DB, model interface{}, field string) (map[string]int64, error) {
	var rows []struct {
		Key   string
		Count int64
	}
	if err := db.Model(model).Select(field + " AS key, COUNT(*) AS count").Group(field).Scan(&rows).Error; err != nil {
		return nil, err
	}
	counts := make(map[string]int64, len(rows))
	for _, row := range rows {
		if row.Key == "" {
			continue
		}
		counts[row.Key] = row.Count
	}
	return counts, nil
}

func slurmAccountRecord(account config.Account) map[string]interface{} {
	return map[string]interface{}{
		"account":             account.Name,
		"name":                account.Name,
		"parent_account":      account.ParentName,
		"users":               account.Users,
		"allowed_qos":         account.AllowQOS,
		"fairshare":           account.Fairshare,
		"max_jobs":            account.MaxJobs,
		"max_submit":          account.MaxSubmit,
		"max_billing_running": account.MaxBillingRunning,
		"max_billing_submit":  account.MaxBillingSubmit,
	}
}

func slurmQOSRecord(qos config.QOS) map[string]interface{} {
	return map[string]interface{}{
		"qos":                          qos.Name,
		"name":                         qos.Name,
		"priority":                     qos.Priority,
		"max_jobs_per_user":            qos.MaxJobsPerUser,
		"max_submit_jobs_per_user":     qos.MaxSubmitJobsPerUser,
		"max_cpu_per_job":              qos.MaxCPUPerJob,
		"max_memory_per_job":           qos.MaxMemoryPerJob,
		"max_billing_per_job":          qos.MaxBillingPerJob,
		"max_billing_per_user_running": qos.MaxBillingPerUserRunning,
		"max_billing_per_user_submit":  qos.MaxBillingPerUserSubmit,
		"max_time":                     qos.MaxTime,
		"preempt":                      qos.Preempt,
	}
}

type slurmTRESAggregate struct {
	Type          string
	Name          string
	TRES          string
	Count         int64
	BillingWeight float64
	Sources       map[string]bool
}

func (h *Handler) slurmTRESRecords() []map[string]interface{} {
	aggregates := make(map[string]*slurmTRESAggregate)
	configSnapshot := h.scheduler.GetSchedulerConfigSnapshot()
	partitions := h.scheduler.ListPartitions("")

	totalCPU := int64(0)
	totalMemory := int64(0)
	totalNodes := int64(0)
	for _, partition := range partitions {
		for _, node := range partition.Nodes {
			totalNodes++
			totalCPU += int64(node.CPU)
			totalMemory += node.Memory
			for _, gres := range node.GRES {
				name, count, ok := slurmTRESResourceCount(gres)
				if !ok {
					continue
				}
				typ, resourceName, tres := slurmCanonicalTRESName(name, aggregates)
				if typ == "" {
					typ, resourceName, tres = "gres", strings.TrimPrefix(name, "gres/"), "gres/"+strings.TrimPrefix(name, "gres/")
				}
				slurmAddTRESAggregate(aggregates, typ, resourceName, tres, count, 0, "node_gres")
			}
		}
	}
	slurmAddTRESAggregate(aggregates, "cpu", "cpu", "cpu", totalCPU, 0, "nodes")
	slurmAddTRESAggregate(aggregates, "mem", "mem", "mem", totalMemory, 0, "nodes")
	slurmAddTRESAggregate(aggregates, "node", "node", "node", totalNodes, 0, "nodes")

	for _, license := range h.scheduler.GetLicenseStatus() {
		name := strings.TrimPrefix(license.Name, "license/")
		slurmAddTRESAggregate(aggregates, "license", name, "license/"+name, int64(license.Total), 0, "licenses")
	}
	for key, weight := range configSnapshot.BillingWeights {
		typ, name, tres := slurmCanonicalTRESName(key, aggregates)
		if typ == "" {
			typ, name, tres = "custom", key, key
		}
		slurmAddTRESAggregate(aggregates, typ, name, tres, 0, weight, "billing_weight")
	}

	keys := make([]string, 0, len(aggregates))
	for key := range aggregates {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left := aggregates[keys[i]]
		right := aggregates[keys[j]]
		if left.Type == right.Type {
			return left.Name < right.Name
		}
		return slurmTRESTypeRank(left.Type) < slurmTRESTypeRank(right.Type)
	})

	records := make([]map[string]interface{}, 0, len(keys))
	for i, key := range keys {
		aggregate := aggregates[key]
		sources := sortedSlurmStringSet(aggregate.Sources)
		source := ""
		if len(sources) > 0 {
			source = strings.Join(sources, ",")
		}
		records = append(records, map[string]interface{}{
			"id":             i + 1,
			"tres":           aggregate.TRES,
			"type":           aggregate.Type,
			"name":           aggregate.Name,
			"count":          aggregate.Count,
			"billing_weight": aggregate.BillingWeight,
			"source":         source,
			"sources":        sources,
			"configured":     aggregate.Count > 0 || aggregate.BillingWeight != 0,
		})
	}
	return records
}

func slurmAddTRESAggregate(aggregates map[string]*slurmTRESAggregate, typ, name, tres string, count int64, billingWeight float64, source string) {
	typ = strings.ToLower(strings.TrimSpace(typ))
	name = strings.TrimSpace(name)
	tres = strings.TrimSpace(tres)
	if typ == "" || name == "" || tres == "" {
		return
	}
	key := strings.ToLower(tres)
	aggregate := aggregates[key]
	if aggregate == nil {
		aggregate = &slurmTRESAggregate{
			Type:    typ,
			Name:    name,
			TRES:    tres,
			Sources: make(map[string]bool),
		}
		aggregates[key] = aggregate
	}
	aggregate.Count += count
	if billingWeight != 0 {
		aggregate.BillingWeight = billingWeight
	}
	if source != "" {
		aggregate.Sources[source] = true
	}
}

func slurmCanonicalTRESName(raw string, existing map[string]*slurmTRESAggregate) (string, string, string) {
	value := strings.ToLower(strings.TrimSpace(raw))
	value = strings.TrimPrefix(value, "tres/")
	switch value {
	case "cpu", "cpus":
		return "cpu", "cpu", "cpu"
	case "mem", "memory", "mem_mb":
		return "mem", "mem", "mem"
	case "node", "nodes":
		return "node", "node", "node"
	}
	switch {
	case strings.HasPrefix(value, "license/"):
		name := strings.TrimPrefix(value, "license/")
		return "license", name, "license/" + name
	case strings.HasPrefix(value, "licenses/"):
		name := strings.TrimPrefix(value, "licenses/")
		return "license", name, "license/" + name
	case strings.HasPrefix(value, "gres/"):
		name := strings.TrimPrefix(value, "gres/")
		return "gres", name, "gres/" + name
	case existing != nil:
		if _, ok := existing["gres/"+value]; ok {
			return "gres", value, "gres/" + value
		}
		if _, ok := existing["license/"+value]; ok {
			return "license", value, "license/" + value
		}
	}
	return "", "", ""
}

func slurmTRESResourceCount(raw string) (string, int64, bool) {
	item := strings.TrimSpace(raw)
	if item == "" {
		return "", 0, false
	}
	count := int64(1)
	if idx := strings.LastIndex(item, ":"); idx > 0 && idx < len(item)-1 {
		parsed, err := strconv.ParseInt(strings.TrimSpace(item[idx+1:]), 10, 64)
		if err == nil && parsed > 0 {
			count = parsed
			item = strings.TrimSpace(item[:idx])
		}
	}
	if item == "" {
		return "", 0, false
	}
	return item, count, true
}

func slurmTRESTypeRank(typ string) int {
	switch typ {
	case "cpu":
		return 0
	case "mem":
		return 1
	case "node":
		return 2
	case "gres":
		return 3
	case "license":
		return 4
	default:
		return 9
	}
}

type slurmAccountingJobSummary struct {
	RecordCount        int
	FirstEvent         string
	LastEvent          string
	FirstAccountingAt  time.Time
	LastAccountingAt   time.Time
	StartTime          time.Time
	EndTime            time.Time
	CPU                int
	Memory             int64
	BillingUnits       float64
	HasStart           bool
	HasTerminal        bool
	TerminalAccounting bool
}

func (h *Handler) slurmSacctmgrConfigRecord() map[string]interface{} {
	configSnapshot := h.scheduler.GetSchedulerConfigSnapshot()
	dbResponding, dbMessage := h.slurmDatabaseStatus()
	partitions := h.scheduler.ListPartitions("")
	clusterRecords := h.slurmClusterRecords()
	return map[string]interface{}{
		"generated_at":       time.Now(),
		"accounting_storage": "database",
		"database_status":    slurmUpDown(dbResponding),
		"database_message":   dbMessage,
		"queue_size":         configSnapshot.QueueSize,
		"backfill":           configSnapshot.Backfill,
		"priority_weights":   configSnapshot.PriorityWeights,
		"billing_weights":    configSnapshot.BillingWeights,
		"fairshare_decay":    configSnapshot.FairshareDecay,
		"cluster_count":      len(clusterRecords),
		"partition_count":    len(partitions),
		"clusters":           clusterRecords,
		"account_count":      len(configSnapshot.Accounts),
		"accounts":           slurmAccountRecords(configSnapshot.Accounts),
		"qos_count":          len(configSnapshot.QOS),
		"qos":                slurmQOSRecords(configSnapshot.QOS),
		"reservation_count":  len(configSnapshot.Reservations),
		"reservations":       slurmReservationRecords(configSnapshot.Reservations),
		"tres":               h.slurmTRESRecords(),
		"licenses":           slurmLicenseRecords(h.scheduler.GetLicenseStatus()),
	}
}

func (h *Handler) slurmSacctmgrStatsRecord() (map[string]interface{}, error) {
	submissionCount, err := slurmCountRows(h.db, &models.Submission{})
	if err != nil {
		return nil, err
	}
	accountingCount, err := slurmCountRows(h.db, &models.AccountingRecord{})
	if err != nil {
		return nil, err
	}
	allocationCount, err := slurmCountRows(h.db, &models.Allocation{})
	if err != nil {
		return nil, err
	}
	stepCount, err := slurmCountRows(h.db, &models.RunStep{})
	if err != nil {
		return nil, err
	}
	triggerCount, err := slurmCountRows(h.db, &models.SlurmTrigger{})
	if err != nil {
		return nil, err
	}
	cronCount, err := slurmCountRows(h.db, &models.SlurmCronJob{})
	if err != nil {
		return nil, err
	}
	jobsByState, err := slurmCountByField(h.db, &models.Submission{}, "status")
	if err != nil {
		return nil, err
	}
	accountingByEvent, err := slurmCountByField(h.db, &models.AccountingRecord{}, "event")
	if err != nil {
		return nil, err
	}
	accountingByState, err := slurmCountByField(h.db, &models.AccountingRecord{}, "state")
	if err != nil {
		return nil, err
	}
	allocationsByState, err := slurmCountByField(h.db, &models.Allocation{}, "status")
	if err != nil {
		return nil, err
	}
	stepsByState, err := slurmCountByField(h.db, &models.RunStep{}, "status")
	if err != nil {
		return nil, err
	}
	dbResponding, dbMessage := h.slurmDatabaseStatus()
	return map[string]interface{}{
		"generated_at":         time.Now(),
		"database_status":      slurmUpDown(dbResponding),
		"database_responding":  dbResponding,
		"database_message":     dbMessage,
		"jobs":                 submissionCount,
		"accounting_records":   accountingCount,
		"allocations":          allocationCount,
		"steps":                stepCount,
		"triggers":             triggerCount,
		"cron_entries":         cronCount,
		"jobs_by_state":        jobsByState,
		"accounting_by_event":  accountingByEvent,
		"accounting_by_state":  accountingByState,
		"allocations_by_state": allocationsByState,
		"steps_by_state":       stepsByState,
		"queue_lengths":        h.scheduler.GetQueueLengths(),
	}, nil
}

func slurmUpDown(ok bool) string {
	if ok {
		return "UP"
	}
	return "DOWN"
}

func slurmCountRows(db *gorm.DB, model interface{}) (int64, error) {
	var count int64
	err := db.Model(model).Count(&count).Error
	return count, err
}

func slurmAccountRecords(accounts []config.Account) []map[string]interface{} {
	records := make([]map[string]interface{}, 0, len(accounts))
	for _, account := range accounts {
		records = append(records, slurmAccountRecord(account))
	}
	return records
}

func slurmQOSRecords(qosItems []config.QOS) []map[string]interface{} {
	records := make([]map[string]interface{}, 0, len(qosItems))
	for _, qos := range qosItems {
		records = append(records, slurmQOSRecord(qos))
	}
	return records
}

func slurmReservationRecords(reservations []config.Reservation) []map[string]interface{} {
	records := make([]map[string]interface{}, 0, len(reservations))
	for _, reservation := range reservations {
		records = append(records, slurmReservationRecord(reservation))
	}
	return records
}

func (h *Handler) slurmFilteredSubmissions(c *gin.Context) ([]models.Submission, error) {
	query := h.db.Model(&models.Submission{}).Order("created_at desc")
	if jobID := firstQuery(c, "job_id", "id"); jobID != "" {
		jobSelectors, err := parseSlurmJobSelectors(jobID)
		if err != nil {
			return nil, err
		}
		query = applySlurmJobSelectorQuery(query, "id", jobSelectors)
	}
	if arrayJobID := c.Query("array_job_id"); arrayJobID != "" {
		query = query.Where("array_job_id IN ?", slurmCSVValues(arrayJobID))
	}
	if arrayTaskID := firstQuery(c, "array_task_id", "array_task"); arrayTaskID != "" {
		taskIDs, err := slurmCSVInts(arrayTaskID)
		if err != nil {
			return nil, err
		}
		query = query.Where("array_task_id IN ?", taskIDs)
	}
	if problemID := firstQuery(c, "problem", "problem_id"); problemID != "" {
		query = query.Where("problem_id IN ?", slurmCSVValues(problemID))
	}
	if userID := firstQuery(c, "user", "user_id"); userID != "" {
		query = query.Where("user_id IN ?", slurmCSVValues(userID))
	}
	if partition := firstQuery(c, "partition", "cluster"); partition != "" {
		query = query.Where("cluster IN ?", slurmCSVValues(partition))
	}
	if jobName := firstQuery(c, "job_name", "name"); jobName != "" {
		jobNames := slurmCSVValues(jobName)
		query = query.Where("job_name IN ? OR (job_name = '' AND problem_id IN ?)", jobNames, jobNames)
	}
	if account := c.Query("account"); account != "" {
		query = query.Where("account IN ?", slurmCSVValues(account))
	}
	if qos := c.Query("qos"); qos != "" {
		query = query.Where("qos IN ?", slurmCSVValues(qos))
	}
	if status := firstQuery(c, "status", "native_status"); status != "" {
		query = query.Where("status IN ?", slurmCSVValues(status))
	}
	var submissions []models.Submission
	if err := query.Find(&submissions).Error; err != nil {
		return nil, err
	}
	return submissions, nil
}

func (h *Handler) slurmAccountingJobSummaries(jobIDs []string) (map[string]slurmAccountingJobSummary, error) {
	out := make(map[string]slurmAccountingJobSummary, len(jobIDs))
	if len(jobIDs) == 0 {
		return out, nil
	}
	var records []models.AccountingRecord
	if err := h.db.Where("submission_id IN ?", jobIDs).Order("created_at asc").Find(&records).Error; err != nil {
		return nil, err
	}
	for _, record := range records {
		summary := out[record.SubmissionID]
		summary.RecordCount++
		if summary.FirstEvent == "" {
			summary.FirstEvent = record.Event
			summary.FirstAccountingAt = record.CreatedAt
		}
		summary.LastEvent = record.Event
		summary.LastAccountingAt = record.CreatedAt
		if slurmAccountingEventStartsWork(record.Event) && !summary.HasStart {
			summary.StartTime = record.CreatedAt
			summary.HasStart = true
		}
		if slurmAccountingEventTerminatesWork(record.Event) {
			summary.EndTime = record.CreatedAt
			summary.HasTerminal = true
			summary.TerminalAccounting = true
		}
		if record.CPU > summary.CPU {
			summary.CPU = record.CPU
		}
		if record.Memory > summary.Memory {
			summary.Memory = record.Memory
		}
		if record.BillingUnits > summary.BillingUnits {
			summary.BillingUnits = record.BillingUnits
		}
		out[record.SubmissionID] = summary
	}
	return out, nil
}

func slurmAccountingEventStartsWork(event string) bool {
	switch event {
	case database.AccountEventStarted, database.AccountEventAllocated, database.AccountEventRunStarted:
		return true
	default:
		return false
	}
}

func slurmAccountingEventTerminatesWork(event string) bool {
	switch event {
	case database.AccountEventCompleted, database.AccountEventFailed, database.AccountEventInterrupted,
		database.AccountEventPreempted, database.AccountEventAllocationReleased,
		database.AccountEventRunCompleted, database.AccountEventRunFailed:
		return true
	default:
		return false
	}
}

func slurmSacctmgrJobRecord(sub *models.Submission, summary slurmAccountingJobSummary) map[string]interface{} {
	record := slurmJobRecord(sub)
	elapsedSeconds := int64(0)
	if !summary.StartTime.IsZero() {
		end := summary.EndTime
		if end.IsZero() && (sub.Status == models.StatusRunning || sub.Status == models.StatusSuspended) {
			end = time.Now()
		}
		if !end.IsZero() && end.After(summary.StartTime) {
			elapsedSeconds = int64(end.Sub(summary.StartTime).Seconds())
		}
	}
	record["job"] = sub.ID
	record["accounting_records"] = summary.RecordCount
	record["first_event"] = summary.FirstEvent
	record["last_event"] = summary.LastEvent
	record["first_accounting_time"] = summary.FirstAccountingAt
	record["last_accounting_time"] = summary.LastAccountingAt
	record["start_time"] = summary.StartTime
	record["end_time"] = summary.EndTime
	record["elapsed_seconds"] = elapsedSeconds
	record["terminal_accounting"] = summary.TerminalAccounting
	record["alloc_cpus"] = firstPositiveInt(summary.CPU, sub.CPU)
	record["alloc_mem"] = firstPositiveInt64(summary.Memory, sub.Memory)
	record["accounting_billing_units"] = summary.BillingUnits
	return record
}

func firstPositiveInt(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func firstPositiveInt64(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func (h *Handler) slurmProblemRecords() ([]map[string]interface{}, error) {
	submissionCounts, err := slurmCountByField(h.db, &models.Submission{}, "problem_id")
	if err != nil {
		return nil, err
	}
	accountingCounts, err := slurmCountByField(h.db, &models.AccountingRecord{}, "problem_id")
	if err != nil {
		return nil, err
	}
	if h.appState == nil {
		return nil, nil
	}
	h.appState.RLock()
	defer h.appState.RUnlock()
	ids := make([]string, 0, len(h.appState.Problems))
	for id := range h.appState.Problems {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	records := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		problem := h.appState.Problems[id]
		if problem == nil {
			continue
		}
		records = append(records, slurmProblemRecord(problem, submissionCounts[id], accountingCounts[id]))
	}
	return records, nil
}

func slurmProblemRecord(problem *judger.Problem, submissions, accountingRecords int64) map[string]interface{} {
	state := slurmProblemState(problem)
	return map[string]interface{}{
		"problem":            problem.ID,
		"problem_id":         problem.ID,
		"name":               problem.Name,
		"level":              problem.Level,
		"partition":          problem.Cluster,
		"cluster":            problem.Cluster,
		"state":              state,
		"start_time":         problem.StartTime,
		"end_time":           problem.EndTime,
		"max_submissions":    problem.MaxSubmissions,
		"cpu":                problem.CPU,
		"cpus":               problem.CPU,
		"memory":             problem.Memory,
		"workflow_steps":     len(problem.Workflow),
		"score_mode":         problem.Score.Mode,
		"account":            problem.Scheduling.Account,
		"qos":                problem.Scheduling.QOS,
		"priority":           problem.Scheduling.Priority,
		"nice":               problem.Scheduling.Nice,
		"hold":               problem.Scheduling.Hold,
		"time_limit":         problem.Scheduling.TimeLimit,
		"dependencies":       problem.Scheduling.Dependencies,
		"reservation":        problem.Scheduling.Reservation,
		"requested_nodelist": problem.Scheduling.NodeList,
		"exclude_nodes":      problem.Scheduling.ExcludeNodes,
		"constraint":         problem.Scheduling.Constraint,
		"gres":               problem.Scheduling.GRES,
		"tres":               problem.Scheduling.TRES,
		"array":              problem.Scheduling.Array,
		"submissions":        submissions,
		"accounting_records": accountingRecords,
	}
}

func slurmProblemState(problem *judger.Problem) string {
	now := time.Now()
	if !problem.StartTime.IsZero() && now.Before(problem.StartTime) {
		return "FUTURE"
	}
	if !problem.EndTime.IsZero() && now.After(problem.EndTime) {
		return "EXPIRED"
	}
	return "ACTIVE"
}

func (h *Handler) slurmResourceRecords() []map[string]interface{} {
	tresRecords := h.slurmTRESRecords()
	records := make([]map[string]interface{}, 0, len(tresRecords))
	for _, tres := range tresRecords {
		record := map[string]interface{}{
			"resource":       tres["tres"],
			"name":           tres["name"],
			"tres":           tres["tres"],
			"type":           tres["type"],
			"server":         "cluster",
			"manager":        "csoj-scheduler",
			"count":          tres["count"],
			"billing_weight": tres["billing_weight"],
			"source":         tres["source"],
			"sources":        tres["sources"],
			"state":          "ACTIVE",
		}
		records = append(records, record)
	}
	return records
}

func (h *Handler) slurmRunawayJobRecords(c *gin.Context) ([]map[string]interface{}, error) {
	var submissions []models.Submission
	query := h.db.Where("status IN ?", []models.Status{models.StatusRunning, models.StatusSuspended}).Order("created_at desc")
	if user := firstQuery(c, "user", "user_id"); user != "" {
		query = query.Where("user_id IN ?", slurmCSVValues(user))
	}
	if partition := firstQuery(c, "partition", "cluster"); partition != "" {
		query = query.Where("cluster IN ?", slurmCSVValues(partition))
	}
	if err := query.Find(&submissions).Error; err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(submissions))
	for _, sub := range submissions {
		ids = append(ids, sub.ID)
	}
	summaries, err := h.slurmAccountingJobSummaries(ids)
	if err != nil {
		return nil, err
	}
	records := make([]map[string]interface{}, 0, len(submissions))
	jobFilter := firstQuery(c, "job_id", "id")
	var selectors slurmJobSelectorSet
	if jobFilter != "" {
		parsed, err := parseSlurmJobSelectors(jobFilter)
		if err != nil {
			return nil, err
		}
		selectors = parsed
	}
	for i := range submissions {
		submissions[i].PopulateSlurmState()
		if selectors.Has() && !selectors.Matches(submissions[i].ID, submissions[i].ArrayJobID, submissions[i].ArrayTaskID) {
			continue
		}
		record := slurmRunawaySubmissionRecord(&submissions[i], summaries[submissions[i].ID])
		if !slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(record["state"]), fmt.Sprint(record["native_status"])) {
			continue
		}
		records = append(records, record)
	}

	var allocations []models.Allocation
	allocationQuery := h.db.Where("status = ?", models.AllocationActive).Order("created_at desc")
	if user := firstQuery(c, "user", "user_id"); user != "" {
		allocationQuery = allocationQuery.Where("user_id IN ?", slurmCSVValues(user))
	}
	if partition := firstQuery(c, "partition", "cluster"); partition != "" {
		allocationQuery = allocationQuery.Where("cluster IN ?", slurmCSVValues(partition))
	}
	if err := allocationQuery.Find(&allocations).Error; err != nil {
		return nil, err
	}
	for i := range allocations {
		if jobFilter != "" && !containsCSVFold(jobFilter, allocations[i].ID) {
			continue
		}
		record := slurmRunawayAllocationRecord(&allocations[i])
		if !slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(record["state"]), fmt.Sprint(record["native_status"])) {
			continue
		}
		records = append(records, record)
	}
	sort.SliceStable(records, func(i, j int) bool {
		return fmt.Sprint(records[i]["started_at"]) > fmt.Sprint(records[j]["started_at"])
	})
	return records, nil
}

func slurmRunawaySubmissionRecord(sub *models.Submission, summary slurmAccountingJobSummary) map[string]interface{} {
	startedAt := summary.StartTime
	if startedAt.IsZero() {
		startedAt = sub.CreatedAt
	}
	elapsed := int64(0)
	if !startedAt.IsZero() {
		elapsed = int64(time.Since(startedAt).Seconds())
	}
	reason := "active job has no terminal accounting event"
	if summary.RecordCount == 0 {
		reason = "active job has no accounting records"
	}
	record := slurmJobRecord(sub)
	record["kind"] = "batch"
	record["started_at"] = startedAt
	record["elapsed_seconds"] = elapsed
	record["accounting_records"] = summary.RecordCount
	record["last_event"] = summary.LastEvent
	record["runaway_candidate"] = !summary.HasTerminal
	record["candidate_reason"] = reason
	return record
}

func slurmRunawayAllocationRecord(allocation *models.Allocation) map[string]interface{} {
	record := slurmAllocationRecord(allocation)
	elapsed := int64(0)
	if !allocation.CreatedAt.IsZero() {
		elapsed = int64(time.Since(allocation.CreatedAt).Seconds())
	}
	record["kind"] = "allocation"
	record["started_at"] = allocation.CreatedAt
	record["elapsed_seconds"] = elapsed
	record["runaway_candidate"] = true
	record["candidate_reason"] = "active allocation has not been released"
	return record
}

func (h *Handler) slurmTransactionRecords(c *gin.Context) ([]map[string]interface{}, error) {
	page, limit := slurmPagination(c, 1, 100, 1000)
	offset := (page - 1) * limit
	filters := slurmAccountingFilters(c)
	if action := firstQuery(c, "action", "transaction", "event"); action != "" {
		filters["event"] = action
	}
	query, err := database.BuildAccountingRecordsQuery(h.db, filters)
	if err != nil {
		return nil, err
	}
	if id := firstQuery(c, "transaction_id", "id"); id != "" {
		query = query.Where("id IN ?", slurmCSVValues(id))
	}
	jobSelectors, err := parseSlurmJobSelectors(firstQuery(c, "job_id", "submission_id"))
	if err != nil {
		return nil, err
	}
	if jobSelectors.Has() {
		query = applySlurmJobSelectorQuery(query, "submission_id", jobSelectors)
	}
	var accounting []models.AccountingRecord
	if err := query.Order("created_at desc").Offset(offset).Limit(limit).Find(&accounting).Error; err != nil {
		return nil, err
	}
	records := make([]map[string]interface{}, 0, len(accounting))
	for _, record := range accounting {
		item := slurmTransactionRecord(record)
		if !slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(item["state"]), fmt.Sprint(item["native_state"])) {
			continue
		}
		records = append(records, item)
	}
	return records, nil
}

func slurmTransactionRecord(record models.AccountingRecord) map[string]interface{} {
	stateRecord := slurmAccountingRecord(record)
	objectType := "job"
	objectID := record.SubmissionID
	if strings.HasPrefix(record.Event, "Run") {
		objectType = "step"
		objectID = firstNonEmpty(record.ContainerID, record.SubmissionID)
	}
	if record.Event == database.AccountEventAllocated || record.Event == database.AccountEventAllocationReleased {
		objectType = "allocation"
		objectID = record.SubmissionID
	}
	return map[string]interface{}{
		"transaction_id":  record.ID,
		"id":              record.ID,
		"timestamp":       record.CreatedAt,
		"action":          record.Event,
		"event":           record.Event,
		"actor":           record.UserID,
		"user_id":         record.UserID,
		"object_type":     objectType,
		"object_id":       objectID,
		"job_id":          record.SubmissionID,
		"container_id":    record.ContainerID,
		"partition":       record.Cluster,
		"cluster":         record.Cluster,
		"node":            record.Node,
		"account":         record.Account,
		"qos":             record.QOS,
		"state":           stateRecord["state"],
		"native_state":    record.State,
		"reason":          record.Reason,
		"message":         record.Message,
		"billing_units":   record.BillingUnits,
		"accounting_time": record.CreatedAt,
	}
}

func (h *Handler) slurmEventRecords(c *gin.Context) ([]map[string]interface{}, error) {
	page, limit := slurmPagination(c, 1, 100, 1000)
	offset := (page - 1) * limit
	filters := slurmAccountingFilters(c)
	if event := firstQuery(c, "event", "type"); event != "" {
		filters["event"] = event
	}
	query, err := database.BuildAccountingRecordsQuery(h.db, filters)
	if err != nil {
		return nil, err
	}
	var accounting []models.AccountingRecord
	if err := query.Order("created_at desc").Offset(offset).Limit(limit).Find(&accounting).Error; err != nil {
		return nil, err
	}
	records := make([]map[string]interface{}, 0, len(accounting))
	for _, record := range accounting {
		item := slurmAccountingEventRecord(record)
		if !slurmEventRecordMatches(c, item) {
			continue
		}
		records = append(records, item)
	}
	for _, item := range h.slurmNodeEventRecords() {
		if !slurmEventRecordMatches(c, item) {
			continue
		}
		records = append(records, item)
	}
	return records, nil
}

func slurmAccountingEventRecord(record models.AccountingRecord) map[string]interface{} {
	stateRecord := slurmAccountingRecord(record)
	return map[string]interface{}{
		"event_id":        fmt.Sprintf("accounting:%d", record.ID),
		"id":              record.ID,
		"source":          "accounting",
		"event":           record.Event,
		"type":            record.Event,
		"timestamp":       record.CreatedAt,
		"job_id":          record.SubmissionID,
		"container_id":    record.ContainerID,
		"user_id":         record.UserID,
		"problem_id":      record.ProblemID,
		"partition":       record.Cluster,
		"cluster":         record.Cluster,
		"node":            record.Node,
		"account":         record.Account,
		"qos":             record.QOS,
		"state":           stateRecord["state"],
		"native_state":    record.State,
		"reason":          record.Reason,
		"message":         record.Message,
		"accounting_time": record.CreatedAt,
	}
}

func (h *Handler) slurmNodeEventRecords() []map[string]interface{} {
	states := h.scheduler.GetClusterStates()
	clusterNames := make([]string, 0, len(states))
	for clusterName := range states {
		clusterNames = append(clusterNames, clusterName)
	}
	sort.Strings(clusterNames)
	records := make([]map[string]interface{}, 0)
	now := time.Now()
	for _, clusterName := range clusterNames {
		nodeNames := make([]string, 0, len(states[clusterName].Nodes))
		for nodeName := range states[clusterName].Nodes {
			nodeNames = append(nodeNames, nodeName)
		}
		sort.Strings(nodeNames)
		for _, nodeName := range nodeNames {
			node := states[clusterName].Nodes[nodeName]
			nodeState := strings.ToUpper(slurmNodeStateFromState(node))
			event := ""
			switch nodeState {
			case "DOWN", "INACTIVE":
				event = "NodeDown"
			case "DRAIN", "DRAINED":
				event = "NodeDrain"
			}
			if event == "" {
				continue
			}
			reason := ""
			if node != nil && node.Node != nil {
				reason = node.Reason
			}
			records = append(records, map[string]interface{}{
				"event_id":     "node:" + clusterName + ":" + nodeName,
				"source":       "scheduler_node",
				"event":        event,
				"type":         event,
				"timestamp":    now,
				"partition":    clusterName,
				"cluster":      clusterName,
				"node":         nodeName,
				"state":        nodeState,
				"native_state": strings.ToLower(nodeState),
				"reason":       reason,
				"message":      reason,
			})
		}
	}
	return records
}

func slurmEventRecordMatches(c *gin.Context, record map[string]interface{}) bool {
	if event := firstQuery(c, "event", "type"); event != "" && !containsCSVFold(event, fmt.Sprint(record["event"])) {
		return false
	}
	if node := firstQuery(c, "node", "nodelist"); node != "" && !containsCSVFold(node, fmt.Sprint(record["node"])) {
		return false
	}
	if partition := firstQuery(c, "partition", "cluster"); partition != "" && !containsCSVFold(partition, fmt.Sprint(record["partition"])) {
		return false
	}
	if user := firstQuery(c, "user", "user_id"); user != "" && !containsCSVFold(user, fmt.Sprint(record["user_id"])) {
		return false
	}
	if !slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(record["state"]), fmt.Sprint(record["native_state"])) {
		return false
	}
	return true
}

func slurmAssociationRecord(association judger.AssociationRecord) map[string]interface{} {
	return map[string]interface{}{
		"account":             association.Account,
		"parent_account":      association.ParentAccount,
		"user":                association.User,
		"user_id":             association.User,
		"qos":                 association.QOS,
		"fairshare":           association.Fairshare,
		"max_jobs":            association.MaxJobs,
		"max_submit":          association.MaxSubmit,
		"max_billing_running": association.MaxBillingRun,
		"max_billing_submit":  association.MaxBillingSub,
	}
}

type slurmUserAggregate struct {
	user         string
	userID       string
	username     string
	principals   map[string]bool
	accounts     map[string]bool
	qos          map[string]bool
	associations []map[string]interface{}
}

func (h *Handler) slurmUserRecords(associations []judger.AssociationRecord) ([]map[string]interface{}, error) {
	var dbUsers []models.User
	if err := h.db.Find(&dbUsers).Error; err != nil {
		return nil, err
	}
	byID := make(map[string]models.User, len(dbUsers))
	byUsername := make(map[string]models.User, len(dbUsers))
	for _, user := range dbUsers {
		byID[strings.ToLower(user.ID)] = user
		byUsername[strings.ToLower(user.Username)] = user
	}

	aggregates := make(map[string]*slurmUserAggregate)
	for _, association := range associations {
		principal := strings.TrimSpace(association.User)
		if principal == "" {
			principal = "*"
		}
		userID, username, key := slurmResolveAssociationUser(principal, byID, byUsername)
		aggregate := aggregates[key]
		if aggregate == nil {
			aggregate = &slurmUserAggregate{
				user:       principal,
				userID:     userID,
				username:   username,
				principals: make(map[string]bool),
				accounts:   make(map[string]bool),
				qos:        make(map[string]bool),
			}
			aggregates[key] = aggregate
		}
		aggregate.principals[principal] = true
		if association.Account != "" {
			aggregate.accounts[association.Account] = true
		}
		if association.QOS != "" {
			aggregate.qos[association.QOS] = true
		}
		aggregate.associations = append(aggregate.associations, slurmAssociationRecord(association))
	}

	keys := make([]string, 0, len(aggregates))
	for key := range aggregates {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return aggregates[keys[i]].user < aggregates[keys[j]].user
	})

	records := make([]map[string]interface{}, 0, len(keys))
	for _, key := range keys {
		aggregate := aggregates[key]
		accounts := sortedSlurmStringSet(aggregate.accounts)
		qosItems := sortedSlurmStringSet(aggregate.qos)
		principals := sortedSlurmStringSet(aggregate.principals)
		defaultAccount := ""
		if len(accounts) > 0 {
			defaultAccount = accounts[0]
		}
		record := map[string]interface{}{
			"user":              aggregate.user,
			"name":              aggregate.user,
			"user_id":           aggregate.userID,
			"username":          aggregate.username,
			"principals":        principals,
			"account":           defaultAccount,
			"default_account":   defaultAccount,
			"accounts":          accounts,
			"qos":               qosItems,
			"allowed_qos":       qosItems,
			"association_count": len(aggregate.associations),
			"associations":      aggregate.associations,
		}
		records = append(records, record)
	}
	return records, nil
}

func slurmResolveAssociationUser(principal string, byID, byUsername map[string]models.User) (string, string, string) {
	if principal == "*" {
		return "*", "*", "wildcard:*"
	}
	if user, ok := byID[strings.ToLower(principal)]; ok {
		return user.ID, user.Username, "id:" + strings.ToLower(user.ID)
	}
	if user, ok := byUsername[strings.ToLower(principal)]; ok {
		return user.ID, user.Username, "id:" + strings.ToLower(user.ID)
	}
	return principal, principal, "principal:" + strings.ToLower(principal)
}

func sortedSlurmStringSet(values map[string]bool) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func slurmReservationRecord(reservation config.Reservation) map[string]interface{} {
	state := "INACTIVE"
	now := time.Now()
	if (reservation.StartTime.IsZero() || !now.Before(reservation.StartTime)) &&
		(reservation.EndTime.IsZero() || now.Before(reservation.EndTime)) {
		state = "ACTIVE"
	}
	if !reservation.EndTime.IsZero() && now.After(reservation.EndTime) {
		state = "EXPIRED"
	}
	duration := float64(0)
	if !reservation.StartTime.IsZero() && !reservation.EndTime.IsZero() {
		duration = reservation.EndTime.Sub(reservation.StartTime).Seconds()
	}
	return map[string]interface{}{
		"reservation":    reservation.Name,
		"name":           reservation.Name,
		"partition":      reservation.Cluster,
		"nodes":          reservation.Nodes,
		"users":          reservation.Users,
		"accounts":       reservation.Accounts,
		"start_time":     reservation.StartTime,
		"end_time":       reservation.EndTime,
		"duration":       duration,
		"cpu":            reservation.CPU,
		"memory":         reservation.Memory,
		"allow_overlap":  reservation.AllowOverlap,
		"ignore_running": reservation.IgnoreRunning,
		"state":          state,
	}
}

func slurmSubmissionJobName(sub *models.Submission) string {
	if sub == nil {
		return ""
	}
	if name := strings.TrimSpace(sub.JobName); name != "" {
		return name
	}
	return sub.ProblemID
}

func slurmQueueJobName(entry judger.QueueEntry) string {
	if name := strings.TrimSpace(entry.JobName); name != "" {
		return name
	}
	return entry.Problem
}

func slurmAccountingJobName(record models.AccountingRecord) string {
	if name := strings.TrimSpace(record.JobName); name != "" {
		return name
	}
	return record.ProblemID
}

func slurmJobNodeCount(nodes int) int {
	if nodes > 0 {
		return nodes
	}
	return 1
}

func slurmTotalCPUFromTaskShape(ntasks, cpusPerTask int) int {
	tasks := 1
	if ntasks > 0 {
		tasks = ntasks
	}
	cpus := 1
	if cpusPerTask > 0 {
		cpus = cpusPerTask
	}
	total := tasks * cpus
	if total <= 0 {
		return 1
	}
	return total
}

func slurmLicenseTRES(licenses string) string {
	items := make([]string, 0)
	for _, raw := range slurmCSVValues(licenses) {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		count := "1"
		if before, after, ok := strings.Cut(name, ":"); ok {
			name = strings.TrimSpace(before)
			if strings.TrimSpace(after) != "" {
				count = strings.TrimSpace(after)
			}
		}
		if before, _, ok := strings.Cut(name, "@"); ok {
			name = strings.TrimSpace(before)
		}
		name = strings.TrimPrefix(name, "licenses/")
		name = strings.TrimPrefix(name, "license/")
		name = strings.TrimPrefix(name, "license:")
		if name == "" {
			continue
		}
		items = append(items, "license/"+name+":"+count)
	}
	return strings.Join(items, ",")
}

func mergeSlurmLicensesIntoTRES(tres, licenses string) string {
	licenseTRES := slurmLicenseTRES(licenses)
	if strings.TrimSpace(licenseTRES) == "" {
		return tres
	}
	if strings.TrimSpace(tres) == "" {
		return licenseTRES
	}
	return strings.TrimSpace(tres) + "," + licenseTRES
}

func stripSlurmLicenseTRES(tres string) string {
	items := make([]string, 0)
	for _, item := range slurmCSVValues(tres) {
		name := item
		if before, _, ok := strings.Cut(item, ":"); ok {
			name = before
		}
		normalized := strings.ToLower(strings.TrimSpace(name))
		if strings.HasPrefix(normalized, "license/") || strings.HasPrefix(normalized, "licenses/") || strings.HasPrefix(normalized, "license:") {
			continue
		}
		items = append(items, item)
	}
	return strings.Join(items, ",")
}

func slurmJobRecord(sub *models.Submission) map[string]interface{} {
	return map[string]interface{}{
		"job_id":               sub.ID,
		"job_id_raw":           sub.ID,
		"array_job_id":         sub.ArrayJobID,
		"array_task_id":        sub.ArrayTaskID,
		"array_task_count":     sub.ArrayTaskCount,
		"array_max_running":    sub.ArrayMaxRunning,
		"partition":            sub.Cluster,
		"name":                 slurmSubmissionJobName(sub),
		"job_name":             slurmSubmissionJobName(sub),
		"problem_id":           sub.ProblemID,
		"user_id":              sub.UserID,
		"state":                sub.SlurmState,
		"native_status":        sub.Status,
		"reason":               sub.SlurmReason,
		"node":                 sub.Node,
		"nodelist":             sub.Node,
		"allocated_cores":      sub.AllocatedCores,
		"allocated_node_cores": sub.AllocatedNodeCores,
		"cpus":                 sub.CPU,
		"ntasks":               sub.NTasks,
		"cpus_per_task":        sub.CPUsPerTask,
		"nodes":                slurmJobNodeCount(sub.Nodes),
		"memory":               sub.Memory,
		"account":              sub.Account,
		"qos":                  sub.QOS,
		"priority":             sub.Priority,
		"nice":                 sub.Nice,
		"hold":                 sub.Hold,
		"begin_time":           sub.BeginTime,
		"deadline":             sub.Deadline,
		"time_limit":           sub.TimeLimit,
		"dependencies":         sub.Dependencies,
		"reservation":          sub.Reservation,
		"requested_nodelist":   sub.NodeList,
		"exclude_nodes":        sub.ExcludeNodes,
		"work_dir":             sub.WorkDir,
		"submit_dir":           sub.WorkDir,
		"chdir":                sub.WorkDir,
		"stdin_path":           sub.StdinPath,
		"input":                sub.StdinPath,
		"stdout_path":          sub.StdoutPath,
		"output":               sub.StdoutPath,
		"stderr_path":          sub.StderrPath,
		"error":                sub.StderrPath,
		"open_mode":            sub.OpenMode,
		"comment":              sub.Comment,
		"mail_type":            sub.MailType,
		"mail_user":            sub.MailUser,
		"exclusive":            sub.Exclusive,
		"requeue":              sub.Requeue,
		"export":               sub.ExportEnv,
		"environment":          sub.Environment,
		"constraint":           sub.Constraint,
		"gres":                 sub.GRES,
		"tres":                 sub.TRES,
		"licenses":             sub.Licenses,
		"billing_units":        sub.BillingUnits,
		"score":                sub.Score,
		"performance":          sub.Performance,
		"submit_time":          sub.CreatedAt,
		"updated_at":           sub.UpdatedAt,
	}
}

func slurmNodeRecord(partition string, node *judger.NodeDetail) map[string]interface{} {
	usedCPU := slurmUsedCPU(node.UsedCores)
	state := slurmEffectiveNodeState(node.State, node.IsPaused, node.CPU, usedCPU)
	return map[string]interface{}{
		"node":         node.Name,
		"partition":    partition,
		"state":        strings.ToUpper(state),
		"native_state": state,
		"paused":       node.IsPaused,
		"cpus":         node.CPU,
		"alloc_cpus":   usedCPU,
		"idle_cpus":    node.CPU - usedCPU,
		"real_memory":  node.Memory,
		"alloc_memory": node.UsedMemory,
		"idle_memory":  node.Memory - node.UsedMemory,
		"features":     node.Features,
		"gres":         node.GRES,
		"weight":       node.Weight,
		"runtime":      judger.NodeRuntimeName(*node.Node),
		"reason":       node.Reason,
		"used_cores":   node.UsedCores,
	}
}

func slurmNodeStateFromState(node *judger.NodeState) string {
	if node == nil || node.Node == nil {
		return "unknown"
	}
	return slurmEffectiveNodeState(node.State, node.IsPaused, node.CPU, slurmUsedCPU(node.UsedCores))
}

func slurmPartitionRecord(partition config.Cluster) map[string]interface{} {
	state := partition.State
	if state == "" {
		state = "up"
	}
	nodeNames := make([]string, 0, len(partition.Nodes))
	totalCPU := 0
	totalMemory := int64(0)
	for _, node := range partition.Nodes {
		nodeNames = append(nodeNames, node.Name)
		totalCPU += node.CPU
		totalMemory += node.Memory
	}
	sort.Strings(nodeNames)
	return map[string]interface{}{
		"partition":      partition.Name,
		"name":           partition.Name,
		"state":          strings.ToUpper(state),
		"native_state":   state,
		"priority_tier":  partition.PriorityTier,
		"max_time":       partition.MaxTime,
		"max_jobs":       partition.MaxJobs,
		"allow_users":    partition.AllowUsers,
		"allow_accounts": partition.AllowAccounts,
		"allow_qos":      partition.AllowQOS,
		"deny_qos":       partition.DenyQOS,
		"nodes":          nodeNames,
		"node_count":     len(nodeNames),
		"total_cpus":     totalCPU,
		"total_memory":   totalMemory,
	}
}

func slurmInfoRecord(partition string, maxTime int, node *judger.NodeState) map[string]interface{} {
	usedCPU := slurmUsedCPU(node.UsedCores)
	state := slurmEffectiveNodeState(node.State, node.IsPaused, node.CPU, usedCPU)
	avail := "up"
	if state == "down" || state == "inactive" {
		avail = "down"
	}
	return map[string]interface{}{
		"partition":    partition,
		"avail":        avail,
		"timelimit":    maxTime,
		"nodes":        1,
		"state":        strings.ToUpper(state),
		"native_state": state,
		"nodelist":     node.Name,
		"node":         node.Name,
		"cpus":         node.CPU,
		"alloc_cpus":   usedCPU,
		"idle_cpus":    node.CPU - usedCPU,
		"memory":       node.Memory,
		"alloc_memory": node.UsedMemory,
		"idle_memory":  node.Memory - node.UsedMemory,
		"features":     node.Features,
		"gres":         node.GRES,
		"runtime":      judger.NodeRuntimeName(*node.Node),
		"reason":       node.Reason,
	}
}

func slurmUsedCPU(usedCores []bool) int {
	usedCPU := 0
	for _, used := range usedCores {
		if used {
			usedCPU++
		}
	}
	return usedCPU
}

func slurmEffectiveNodeState(baseState string, paused bool, totalCPU, usedCPU int) string {
	state := strings.ToLower(strings.TrimSpace(baseState))
	switch state {
	case "down", "inactive":
		return state
	case "drain", "drained":
		return state
	}
	if paused {
		return "drain"
	}
	if totalCPU > 0 {
		switch {
		case usedCPU >= totalCPU:
			return "allocated"
		case usedCPU > 0:
			return "mixed"
		}
	}
	if state == "" || state == "up" {
		return "idle"
	}
	return state
}

func slurmAllocationRecord(allocation *models.Allocation) map[string]interface{} {
	state := "ALLOCATED"
	if allocation.Status == models.AllocationReleased {
		state = "RELEASED"
	}
	nodeCount := slurmAllocationNodeCount(allocation)
	env := map[string]string{
		"SLURM_JOB_ID":            allocation.ID,
		"SLURM_JOB_PARTITION":     allocation.Cluster,
		"SLURM_JOB_NODELIST":      allocation.Node,
		"SLURM_JOB_NUM_NODES":     strconv.Itoa(nodeCount),
		"SLURM_NNODES":            strconv.Itoa(nodeCount),
		"SLURM_CPUS_ON_NODE":      strconv.Itoa(slurmAllocationPrimaryCPU(allocation)),
		"SLURM_JOB_CPUS_PER_NODE": slurmAllocationCPUsPerNode(allocation),
		"SLURM_TRES_PER_JOB":      allocation.TRES,
	}
	return map[string]interface{}{
		"allocation_id":        allocation.ID,
		"job_id":               allocation.ID,
		"state":                state,
		"native_status":        allocation.Status,
		"user_id":              allocation.UserID,
		"partition":            allocation.Cluster,
		"node":                 allocation.Node,
		"nodelist":             allocation.Node,
		"nodes":                nodeCount,
		"cpus":                 allocation.CPU,
		"memory":               allocation.Memory,
		"allocated_cores":      allocation.AllocatedCores,
		"allocated_node_cores": allocation.AllocatedNodeCores,
		"account":              allocation.Account,
		"qos":                  allocation.QOS,
		"tres":                 allocation.TRES,
		"gres":                 allocation.GRES,
		"billing_units":        allocation.BillingUnits,
		"time_limit":           allocation.TimeLimit,
		"constraint":           allocation.Constraint,
		"reservation":          allocation.Reservation,
		"requested_nodelist":   allocation.NodeList,
		"exclude_nodes":        allocation.ExcludeNodes,
		"exclusive":            allocation.Exclusive,
		"reason":               allocation.Reason,
		"created_at":           allocation.CreatedAt,
		"released_at":          allocation.ReleasedAt,
		"env":                  env,
	}
}

func slurmAllocationNodeCount(allocation *models.Allocation) int {
	if allocation == nil {
		return 1
	}
	if allocation.Nodes > 0 {
		return allocation.Nodes
	}
	if nodes := strings.Split(strings.TrimSpace(allocation.Node), ","); len(nodes) > 0 && strings.TrimSpace(nodes[0]) != "" {
		return len(nodes)
	}
	return 1
}

func slurmAllocationPrimaryCPU(allocation *models.Allocation) int {
	if allocation == nil {
		return 0
	}
	cores := parseSlurmAllocatedCores(allocation.AllocatedCores)
	if len(cores) > 0 {
		return len(cores)
	}
	return allocation.CPU
}

func slurmAllocationCPUsPerNode(allocation *models.Allocation) string {
	if allocation == nil {
		return ""
	}
	if strings.TrimSpace(allocation.AllocatedNodeCores) == "" {
		return strconv.Itoa(slurmAllocationPrimaryCPU(allocation))
	}
	parts := strings.Split(allocation.AllocatedNodeCores, ";")
	counts := make([]string, 0, len(parts))
	for _, part := range parts {
		_, cores, ok := strings.Cut(part, ":")
		if !ok {
			continue
		}
		counts = append(counts, strconv.Itoa(len(parseSlurmAllocatedCores(cores))))
	}
	if len(counts) == 0 {
		return strconv.Itoa(slurmAllocationPrimaryCPU(allocation))
	}
	return strings.Join(counts, ",")
}

func parseSlurmAllocatedCores(value string) []int {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	cores := make([]int, 0, len(parts))
	for _, part := range parts {
		coreID, err := strconv.Atoi(strings.TrimSpace(part))
		if err == nil {
			cores = append(cores, coreID)
		}
	}
	return cores
}

func slurmRunStepRecord(step *models.RunStep) map[string]interface{} {
	state, reason := models.DeriveSlurmJobState(step.Status, false, step.Reason)
	return map[string]interface{}{
		"step_id":         step.ID,
		"allocation_id":   step.AllocationID,
		"job_id":          step.AllocationID,
		"job_step_id":     step.AllocationID + "." + step.ID,
		"container_id":    step.ContainerID,
		"user_id":         step.UserID,
		"partition":       step.Cluster,
		"node":            step.Node,
		"nodelist":        step.Node,
		"image":           step.Image,
		"runtime":         step.Runtime,
		"command":         step.Command,
		"state":           state,
		"native_status":   step.Status,
		"reason":          reason,
		"exit_code":       step.ExitCode,
		"stdout":          step.Stdout,
		"stderr":          step.Stderr,
		"timeout":         step.Timeout,
		"cpus":            step.CPU,
		"memory":          step.Memory,
		"allocated_cores": step.AllocatedCores,
		"alloc_tres":      slurmStepAllocTRES(step),
		"elapsed_seconds": slurmRunStepElapsedSeconds(step),
		"started_at":      step.StartedAt,
		"finished_at":     step.FinishedAt,
		"created_at":      step.CreatedAt,
	}
}

func matchesSlurmStepFilters(step *models.RunStep, record map[string]interface{}, c *gin.Context) bool {
	if stepID := firstQuery(c, "step_id", "id", "job_step_id"); stepID != "" {
		matched := false
		for _, value := range slurmCSVValues(stepID) {
			if slurmAttachStepID(value) == step.ID || value == step.AllocationID+"."+step.ID {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if user := firstQuery(c, "user", "user_id"); user != "" && !containsCSVFold(user, step.UserID) {
		return false
	}
	if partition := firstQuery(c, "partition", "cluster"); partition != "" && !containsCSVFold(partition, step.Cluster) {
		return false
	}
	if node := firstQuery(c, "node", "nodelist"); node != "" && !containsCSVFold(node, step.Node) {
		return false
	}
	return slurmStateFilterMatches(slurmStateQuery(c), fmt.Sprint(record["state"]), fmt.Sprint(record["native_status"]))
}

func slurmAttachRecord(step *models.RunStep) map[string]interface{} {
	record := slurmRunStepRecord(step)
	record["attached"] = true
	record["stdin_supported"] = false
	record["stdout_bytes"] = len(step.Stdout)
	record["stderr_bytes"] = len(step.Stderr)
	record["job_step_id"] = step.AllocationID + "." + step.ID
	return record
}

func slurmStepStatRecord(step *models.RunStep) map[string]interface{} {
	state, reason := models.DeriveSlurmJobState(step.Status, false, step.Reason)
	elapsedSeconds := slurmRunStepElapsedSeconds(step)
	return map[string]interface{}{
		"step_id":         step.ID,
		"job_id":          step.AllocationID,
		"allocation_id":   step.AllocationID,
		"job_step_id":     step.AllocationID + "." + step.ID,
		"container_id":    step.ContainerID,
		"user_id":         step.UserID,
		"partition":       step.Cluster,
		"node":            step.Node,
		"nodelist":        step.Node,
		"state":           state,
		"native_status":   step.Status,
		"reason":          reason,
		"alloc_cpus":      step.CPU,
		"cpus":            step.CPU,
		"alloc_memory":    step.Memory,
		"memory":          step.Memory,
		"allocated_cores": step.AllocatedCores,
		"alloc_tres":      slurmStepAllocTRES(step),
		"elapsed_seconds": elapsedSeconds,
		"ave_cpu":         step.AveCPU,
		"ave_rss":         step.AveRSS,
		"max_rss":         step.MaxRSS,
		"max_vmsize":      step.MaxVMSize,
		"exit_code":       step.ExitCode,
		"started_at":      step.StartedAt,
		"finished_at":     step.FinishedAt,
		"created_at":      step.CreatedAt,
	}
}

func slurmRunStepElapsedSeconds(step *models.RunStep) int64 {
	if step == nil || step.StartedAt.IsZero() {
		return 0
	}
	end := step.FinishedAt
	if end.IsZero() {
		end = time.Now()
	}
	if end.Before(step.StartedAt) {
		return 0
	}
	return int64(end.Sub(step.StartedAt).Seconds())
}

func slurmStepAllocTRES(step *models.RunStep) string {
	if step == nil {
		return ""
	}
	parts := make([]string, 0, 2)
	if step.CPU > 0 {
		parts = append(parts, "cpu="+strconv.Itoa(step.CPU))
	}
	if step.Memory > 0 {
		parts = append(parts, "mem="+strconv.FormatInt(step.Memory, 10)+"M")
	}
	return strings.Join(parts, ",")
}

func bindSlurmHostlistRequest(c *gin.Context) (slurmHostlistRequest, error) {
	req := slurmHostlistRequest{
		Hostlist:  c.Query("hostlist"),
		NodeList:  c.Query("node_list"),
		Nodelist:  c.Query("nodelist"),
		NodesList: c.Query("nodeslist"),
	}
	if nodes := firstQuery(c, "nodes", "node"); nodes != "" {
		req.Nodes = slurmStringList(slurmCSVValues(nodes))
	}
	if hostnames := c.Query("hostnames"); hostnames != "" {
		req.Hostnames = slurmStringList(slurmCSVValues(hostnames))
	}
	if c.Request.Method != http.MethodGet && c.Request.ContentLength != 0 {
		if err := c.ShouldBindJSON(&req); err != nil {
			return slurmHostlistRequest{}, err
		}
	}
	return req, nil
}

func (h *Handler) slurmConfiguredHostnames() []string {
	states := h.scheduler.GetClusterStates()
	hostnames := make([]string, 0)
	for _, cluster := range states {
		for nodeName := range cluster.Nodes {
			hostnames = append(hostnames, nodeName)
		}
	}
	sort.Strings(hostnames)
	return slurmUniqueHostnames(hostnames)
}

func slurmHostlistText(req slurmHostlistRequest) string {
	if hostlist := firstNonEmpty(req.Hostlist, req.NodeList, req.Nodelist, req.NodesList); hostlist != "" {
		return hostlist
	}
	values := append([]string(nil), req.Hostnames.Strings()...)
	values = append(values, req.Nodes.Strings()...)
	return strings.Join(values, ",")
}

func slurmHostlistRecord(hostlist string, hostnames []string) map[string]interface{} {
	hostnames = slurmUniqueHostnames(hostnames)
	return map[string]interface{}{
		"hostlist":  hostlist,
		"nodelist":  hostlist,
		"hostnames": hostnames,
		"nodes":     hostnames,
		"count":     len(hostnames),
	}
}

func slurmExpandHostlist(value string) ([]string, error) {
	parts := slurmListValues(value)
	hostnames := make([]string, 0, len(parts))
	seen := make(map[string]bool)
	for _, part := range parts {
		expanded, err := slurmExpandHostPattern(part)
		if err != nil {
			return nil, err
		}
		for _, nodeName := range expanded {
			nodeName = strings.TrimSpace(nodeName)
			if nodeName == "" || seen[nodeName] {
				continue
			}
			seen[nodeName] = true
			hostnames = append(hostnames, nodeName)
		}
	}
	return hostnames, nil
}

func slurmExpandHostPattern(pattern string) ([]string, error) {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return nil, nil
	}
	open := strings.Index(pattern, "[")
	if open == -1 {
		if strings.Contains(pattern, "]") {
			return nil, fmt.Errorf("unmatched hostlist bracket in %q", pattern)
		}
		return []string{pattern}, nil
	}
	close := strings.Index(pattern[open:], "]")
	if close == -1 {
		return nil, fmt.Errorf("unmatched hostlist bracket in %q", pattern)
	}
	close += open
	prefix := pattern[:open]
	body := pattern[open+1 : close]
	suffix := pattern[close+1:]
	values, err := slurmExpandHostRangeBody(body)
	if err != nil {
		return nil, err
	}
	suffixes, err := slurmExpandHostPattern(suffix)
	if err != nil {
		return nil, err
	}
	if len(suffixes) == 0 {
		suffixes = []string{""}
	}
	out := make([]string, 0, len(values)*len(suffixes))
	for _, value := range values {
		for _, expandedSuffix := range suffixes {
			out = append(out, prefix+value+expandedSuffix)
		}
	}
	return out, nil
}

func slurmExpandHostRangeBody(body string) ([]string, error) {
	parts := slurmListValues(body)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty hostlist range")
	}
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		values, err := slurmExpandHostRangePart(part)
		if err != nil {
			return nil, err
		}
		out = append(out, values...)
	}
	return out, nil
}

func slurmExpandHostRangePart(part string) ([]string, error) {
	part = strings.TrimSpace(part)
	if part == "" {
		return nil, fmt.Errorf("empty hostlist range part")
	}
	rangePart, stepPart, _ := strings.Cut(part, ":")
	step := 1
	if stepPart != "" {
		parsedStep, err := strconv.Atoi(strings.TrimSpace(stepPart))
		if err != nil || parsedStep <= 0 {
			return nil, fmt.Errorf("invalid hostlist range step %q", stepPart)
		}
		step = parsedStep
	}
	startRaw, endRaw, hasRange := strings.Cut(rangePart, "-")
	if !hasRange {
		return []string{rangePart}, nil
	}
	start, err := strconv.Atoi(startRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid hostlist range start %q", startRaw)
	}
	end, err := strconv.Atoi(endRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid hostlist range end %q", endRaw)
	}
	width := len(startRaw)
	if len(endRaw) > width {
		width = len(endRaw)
	}
	out := make([]string, 0)
	if start <= end {
		for value := start; value <= end; value += step {
			out = append(out, fmt.Sprintf("%0*d", width, value))
		}
		return out, nil
	}
	for value := start; value >= end; value -= step {
		out = append(out, fmt.Sprintf("%0*d", width, value))
	}
	return out, nil
}

type slurmHostnameToken struct {
	name    string
	prefix  string
	number  int
	width   int
	hasTail bool
}

func slurmCompressHostnames(hostnames []string) string {
	hostnames = slurmUniqueHostnames(hostnames)
	sort.Strings(hostnames)

	groups := make(map[string][]slurmHostnameToken)
	literals := make([]string, 0)
	for _, hostname := range hostnames {
		token := slurmParseHostnameToken(hostname)
		if !token.hasTail {
			literals = append(literals, hostname)
			continue
		}
		key := token.prefix + "\x00" + strconv.Itoa(token.width)
		groups[key] = append(groups[key], token)
	}

	fragments := append([]string(nil), literals...)
	groupKeys := make([]string, 0, len(groups))
	for key := range groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)
	for _, key := range groupKeys {
		fragments = append(fragments, slurmCompressHostnameGroup(groups[key]))
	}
	sort.Strings(fragments)
	return strings.Join(fragments, ",")
}

func slurmParseHostnameToken(hostname string) slurmHostnameToken {
	idx := len(hostname)
	for idx > 0 {
		ch := hostname[idx-1]
		if ch < '0' || ch > '9' {
			break
		}
		idx--
	}
	if idx == len(hostname) {
		return slurmHostnameToken{name: hostname}
	}
	numberText := hostname[idx:]
	number, err := strconv.Atoi(numberText)
	if err != nil {
		return slurmHostnameToken{name: hostname}
	}
	return slurmHostnameToken{
		name:    hostname,
		prefix:  hostname[:idx],
		number:  number,
		width:   len(numberText),
		hasTail: true,
	}
}

func slurmCompressHostnameGroup(tokens []slurmHostnameToken) string {
	sort.Slice(tokens, func(i, j int) bool {
		if tokens[i].number == tokens[j].number {
			return tokens[i].name < tokens[j].name
		}
		return tokens[i].number < tokens[j].number
	})
	if len(tokens) == 1 {
		return tokens[0].name
	}

	prefix := tokens[0].prefix
	width := tokens[0].width
	ranges := make([]string, 0)
	start := tokens[0].number
	prev := tokens[0].number
	for i := 1; i <= len(tokens); i++ {
		if i < len(tokens) && tokens[i].number == prev+1 {
			prev = tokens[i].number
			continue
		}
		if start == prev {
			ranges = append(ranges, fmt.Sprintf("%0*d", width, start))
		} else {
			ranges = append(ranges, fmt.Sprintf("%0*d-%0*d", width, start, width, prev))
		}
		if i < len(tokens) {
			start = tokens[i].number
			prev = tokens[i].number
		}
	}
	return prefix + "[" + strings.Join(ranges, ",") + "]"
}

func slurmUniqueHostnames(hostnames []string) []string {
	seen := make(map[string]bool, len(hostnames))
	out := make([]string, 0, len(hostnames))
	for _, hostname := range hostnames {
		hostname = strings.TrimSpace(hostname)
		if hostname == "" || seen[hostname] {
			continue
		}
		seen[hostname] = true
		out = append(out, hostname)
	}
	return out
}

func slurmFieldsQuery(c *gin.Context) string {
	return firstQuery(c, "fields", "format", "Format", "o", "O")
}

func slurmProjectFields(records []map[string]interface{}, fields string) []map[string]interface{} {
	fieldList := slurmFieldList(fields)
	if len(fieldList) == 0 {
		return records
	}
	projected := make([]map[string]interface{}, 0, len(records))
	for _, record := range records {
		projected = append(projected, slurmProjectOne(record, fields))
	}
	return projected
}

func slurmProjectOne(record map[string]interface{}, fields string) map[string]interface{} {
	fieldList := slurmFieldList(fields)
	if len(fieldList) == 0 {
		return record
	}
	out := make(map[string]interface{}, len(fieldList))
	for _, field := range fieldList {
		if value, ok := record[field]; ok {
			out[field] = value
		}
	}
	return out
}

func slurmFieldList(fields string) []string {
	if strings.TrimSpace(fields) == "" {
		return nil
	}
	parts := slurmListValues(fields)
	out := make([]string, 0, len(parts))
	seen := make(map[string]bool)
	for _, part := range parts {
		field := slurmProjectionFieldName(part)
		if field == "all" || field == "*" {
			return nil
		}
		if field != "" {
			if seen[field] {
				continue
			}
			seen[field] = true
			out = append(out, field)
		}
	}
	return out
}

func slurmProjectionFieldName(raw string) string {
	field := strings.TrimSpace(raw)
	field = strings.Trim(field, `"'`)
	if field == "" {
		return ""
	}
	if strings.HasPrefix(field, "%") {
		field = strings.TrimPrefix(field, "%")
		for len(field) > 0 {
			ch := field[0]
			if ch == '.' || ch == '-' || (ch >= '0' && ch <= '9') {
				field = field[1:]
				continue
			}
			break
		}
	}
	field = strings.TrimSpace(field)
	switch field {
	case "i", "I":
		return "job_id"
	case "P", "p":
		return "partition"
	case "j", "J":
		return "job_name"
	case "u", "U":
		return "user_id"
	case "t", "T":
		return "state"
	case "D", "d":
		return "nodes"
	case "R", "r":
		return "reason"
	case "N", "n":
		return "nodelist"
	case "C", "c":
		return "cpus"
	case "m":
		return "memory"
	case "Q":
		return "priority"
	case "l", "L":
		return "timelimit"
	case "V":
		return "submit_time"
	}

	compact := strings.ToLower(field)
	compact = strings.ReplaceAll(compact, "_", "")
	compact = strings.ReplaceAll(compact, "-", "")
	compact = strings.ReplaceAll(compact, ".", "")
	compact = strings.ReplaceAll(compact, " ", "")
	switch compact {
	case "", "none":
		return ""
	case "all", "*":
		return "all"
	case "job", "jobid", "id":
		return "job_id"
	case "jobidraw":
		return "job_id_raw"
	case "submissionid":
		return "submission_id"
	case "arrayjob", "arrayjobid":
		return "array_job_id"
	case "arraytask", "arraytaskid":
		return "array_task_id"
	case "arraytaskcount":
		return "array_task_count"
	case "arraymaxrunning":
		return "array_max_running"
	case "jobname":
		return "job_name"
	case "problem", "problemid":
		return "problem_id"
	case "userid":
		return "user_id"
	case "username", "login":
		return "username"
	case "partitionname", "cluster":
		return "partition"
	case "nativestatus":
		return "native_status"
	case "nativestate":
		return "native_state"
	case "statecompact":
		return "state"
	case "reasonlist":
		return "reason"
	case "hostlist":
		return "hostlist"
	case "nodeslist":
		return "nodelist"
	case "reqnodelist", "requestednodelist", "requestnodelist":
		return "requested_nodelist"
	case "excnodelist", "excludednodes", "excludenodes":
		return "exclude_nodes"
	case "nodename":
		return "node"
	case "cpu", "numcpus":
		return "cpus"
	case "alloccpus", "allocatedcpus":
		return "alloc_cpus"
	case "idlecpus":
		return "idle_cpus"
	case "ntasks", "tasks":
		return "ntasks"
	case "cpuspertask":
		return "cpus_per_task"
	case "mem", "minmemory":
		return "memory"
	case "allocmem":
		return "alloc_mem"
	case "allocmemory":
		return "alloc_memory"
	case "idlememory":
		return "idle_memory"
	case "realmemory":
		return "real_memory"
	case "billing", "billingunits":
		return "billing_units"
	case "prioritylong":
		return "priority"
	case "timelimit":
		return "timelimit"
	case "timelimitseconds", "timelimitsec":
		return "time_limit"
	case "submittime":
		return "submit_time"
	case "elapsed", "elapsedtime", "elapsedseconds":
		return "elapsed_seconds"
	case "exit", "exitcode":
		return "exit_code"
	case "step", "stepid":
		return "step_id"
	case "jobstep", "jobstepid":
		return "job_step_id"
	case "allocation", "allocationid":
		return "allocation_id"
	case "container", "containerid":
		return "container_id"
	case "alloctres":
		return "alloc_tres"
	case "averagescpu", "averagecpu", "avecpu":
		return "ave_cpu"
	case "averagerss", "averss":
		return "ave_rss"
	case "maxrss":
		return "max_rss"
	case "maxvmsize":
		return "max_vmsize"
	case "allowedqos":
		return "allowed_qos"
	case "rawshares":
		return "raw_shares"
	case "normalizedshares":
		return "normalized_shares"
	case "rawusage":
		return "raw_usage"
	case "effectiveusage":
		return "effective_usage"
	case "usagepenalty":
		return "usage_penalty"
	case "runningjobs":
		return "running_jobs"
	case "submittedjobs":
		return "submitted_jobs"
	case "starttime":
		return "start_time"
	case "endtime":
		return "end_time"
	case "startedat":
		return "started_at"
	case "finishedat":
		return "finished_at"
	case "maxjobs":
		return "max_jobs"
	case "maxsubmit":
		return "max_submit"
	case "maxbillingrunning":
		return "max_billing_running"
	case "maxbillingsubmit":
		return "max_billing_submit"
	default:
		return strings.ToLower(strings.ReplaceAll(field, "-", "_"))
	}
}

func slurmPagination(c *gin.Context, defaultPage, defaultLimit, maxLimit int) (int, int) {
	page, err := strconv.Atoi(c.DefaultQuery("page", strconv.Itoa(defaultPage)))
	if err != nil || page < 1 {
		page = defaultPage
	}
	limit, err := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(defaultLimit)))
	if err != nil || limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	return page, limit
}

func firstQuery(c *gin.Context, names ...string) string {
	for _, name := range names {
		if value := c.Query(name); value != "" {
			return value
		}
	}
	return ""
}

func slurmAccountingFilters(c *gin.Context) map[string]string {
	return map[string]string{
		"user_id":       firstQuery(c, "user", "user_id"),
		"problem_id":    c.Query("problem_id"),
		"job_name":      c.Query("job_name"),
		"cluster":       firstQuery(c, "partition", "cluster"),
		"node":          firstQuery(c, "node", "nodelist"),
		"account":       c.Query("account"),
		"qos":           c.Query("qos"),
		"array_job_id":  c.Query("array_job_id"),
		"array_task_id": c.Query("array_task_id"),
		"event":         c.Query("event"),
		"state":         c.Query("native_state"),
		"start_time":    firstQuery(c, "start_time", "starttime", "start"),
		"end_time":      firstQuery(c, "end_time", "endtime", "end"),
	}
}

type slurmTriggerEvaluation struct {
	Matched bool
	Count   int
	Message string
}

func slurmTriggerFromRequest(req slurmTriggerRequest) (*models.SlurmTrigger, error) {
	event, err := slurmCanonicalTriggerEvent(firstNonEmpty(req.Event, req.Type))
	if err != nil {
		return nil, err
	}
	active := true
	if req.Active != nil {
		active = *req.Active
	}
	id := firstNonEmpty(req.ID, req.TriggerID)
	if strings.TrimSpace(id) == "" {
		id = "trigger-" + uuid.NewString()
	}
	return &models.SlurmTrigger{
		ID:        strings.TrimSpace(id),
		Name:      strings.TrimSpace(req.Name),
		Event:     event,
		JobID:     strings.TrimSpace(req.JobID),
		UserID:    strings.TrimSpace(firstNonEmpty(req.UserID, req.User)),
		Partition: strings.TrimSpace(firstNonEmpty(req.Partition, req.Cluster)),
		Node:      strings.TrimSpace(req.Node),
		State:     strings.TrimSpace(req.State),
		Action:    strings.TrimSpace(req.Action),
		Program:   strings.TrimSpace(req.Program),
		Flags:     strings.TrimSpace(req.Flags),
		Active:    active,
	}, nil
}

func slurmCanonicalTriggerEvents(value string) []string {
	values := slurmCSVValues(value)
	events := make([]string, 0, len(values))
	for _, item := range values {
		event, err := slurmCanonicalTriggerEvent(item)
		if err == nil && event != "" {
			events = append(events, event)
		}
	}
	return events
}

func slurmCanonicalTriggerEvent(value string) (string, error) {
	token := strings.ToLower(strings.TrimSpace(value))
	token = strings.ReplaceAll(token, "-", "_")
	token = strings.ReplaceAll(token, ".", "_")
	switch token {
	case "", "job_end", "jobend", "job_fini", "jobfini", "fini", "end":
		if token == "" {
			return "", fmt.Errorf("trigger event is required")
		}
		return "job_end", nil
	case "job_fail", "jobfail", "fail", "failure":
		return "job_fail", nil
	case "job_cancel", "jobcancel", "cancel", "cancelled", "canceled":
		return "job_cancel", nil
	case "job_time_limit", "job_timelimit", "time_limit", "timelimit", "timeout":
		return "job_time_limit", nil
	case "job_state", "jobstate", "state":
		return "job_state", nil
	case "node_down", "nodedown", "down":
		return "node_down", nil
	case "node_drain", "nodedrain", "drain", "drained", "draining":
		return "node_drain", nil
	case "node_up", "nodeup", "up", "idle", "resume":
		return "node_up", nil
	case "allocation_release", "allocationrelease", "alloc_release", "allocation_released":
		return "allocation_release", nil
	case "run_end", "runend", "step_end", "stepend":
		return "run_end", nil
	default:
		return "", fmt.Errorf("unsupported strigger event %q", value)
	}
}

func slurmTriggerRecord(trigger *models.SlurmTrigger) map[string]interface{} {
	return map[string]interface{}{
		"trigger_id":  trigger.ID,
		"id":          trigger.ID,
		"name":        trigger.Name,
		"event":       trigger.Event,
		"job_id":      trigger.JobID,
		"user_id":     trigger.UserID,
		"partition":   trigger.Partition,
		"node":        trigger.Node,
		"state":       trigger.State,
		"action":      trigger.Action,
		"program":     trigger.Program,
		"flags":       trigger.Flags,
		"active":      trigger.Active,
		"fired_at":    trigger.FiredAt,
		"match_count": trigger.MatchCount,
		"message":     trigger.Message,
		"created_at":  trigger.CreatedAt,
		"updated_at":  trigger.UpdatedAt,
	}
}

func (h *Handler) slurmEvaluateTrigger(trigger *models.SlurmTrigger) (slurmTriggerEvaluation, error) {
	switch trigger.Event {
	case "job_state":
		return h.slurmEvaluateJobStateTrigger(trigger)
	case "job_end":
		return h.slurmEvaluateAccountingTrigger(trigger, []string{
			database.AccountEventCompleted,
			database.AccountEventFailed,
			database.AccountEventInterrupted,
			database.AccountEventPreempted,
		}, "")
	case "job_fail":
		return h.slurmEvaluateAccountingTrigger(trigger, []string{
			database.AccountEventFailed,
			database.AccountEventRunFailed,
		}, "")
	case "job_cancel":
		return h.slurmEvaluateAccountingTrigger(trigger, []string{database.AccountEventInterrupted}, "")
	case "job_time_limit":
		return h.slurmEvaluateAccountingTrigger(trigger, []string{
			database.AccountEventFailed,
			database.AccountEventInterrupted,
			database.AccountEventAllocationReleased,
			database.AccountEventRunFailed,
		}, "TimeLimit")
	case "allocation_release":
		return h.slurmEvaluateAllocationReleaseTrigger(trigger)
	case "run_end":
		return h.slurmEvaluateRunEndTrigger(trigger)
	case "node_down", "node_drain", "node_up":
		return h.slurmEvaluateNodeTrigger(trigger)
	default:
		return slurmTriggerEvaluation{}, fmt.Errorf("unsupported strigger event %q", trigger.Event)
	}
}

func (h *Handler) slurmEvaluateJobStateTrigger(trigger *models.SlurmTrigger) (slurmTriggerEvaluation, error) {
	query := h.db.Model(&models.Submission{})
	var err error
	query, err = applySlurmTriggerJobFilters(query, trigger, "id")
	if err != nil {
		return slurmTriggerEvaluation{}, err
	}
	var submissions []models.Submission
	if err := query.Find(&submissions).Error; err != nil {
		return slurmTriggerEvaluation{}, err
	}
	count := 0
	for i := range submissions {
		state, _ := models.DeriveSlurmJobState(submissions[i].Status, submissions[i].Hold, submissions[i].Reason)
		if trigger.State == "" || slurmStateFilterMatches(trigger.State, state, string(submissions[i].Status)) {
			count++
		}
	}
	return slurmTriggerEvaluation{Matched: count > 0, Count: count, Message: fmt.Sprintf("%d jobs match state trigger", count)}, nil
}

func (h *Handler) slurmEvaluateAccountingTrigger(trigger *models.SlurmTrigger, events []string, reason string) (slurmTriggerEvaluation, error) {
	query := h.db.Model(&models.AccountingRecord{}).Where("event IN ?", events)
	var err error
	query, err = applySlurmTriggerAccountingFilters(query, trigger)
	if err != nil {
		return slurmTriggerEvaluation{}, err
	}
	if reason != "" {
		query = query.Where("reason = ? OR message = ?", reason, reason)
	}
	var count int64
	if err := query.Count(&count).Error; err != nil {
		return slurmTriggerEvaluation{}, err
	}
	return slurmTriggerEvaluation{Matched: count > 0, Count: int(count), Message: fmt.Sprintf("%d accounting events match trigger", count)}, nil
}

func (h *Handler) slurmEvaluateAllocationReleaseTrigger(trigger *models.SlurmTrigger) (slurmTriggerEvaluation, error) {
	query := h.db.Model(&models.Allocation{}).Where("status = ?", models.AllocationReleased)
	if trigger.JobID != "" {
		query = query.Where("id IN ?", slurmCSVValues(trigger.JobID))
	}
	if trigger.UserID != "" {
		query = query.Where("user_id IN ?", slurmCSVValues(trigger.UserID))
	}
	if trigger.Partition != "" {
		query = query.Where("cluster IN ?", slurmCSVValues(trigger.Partition))
	}
	if trigger.Node != "" {
		query = query.Where("node IN ?", slurmCSVValues(trigger.Node))
	}
	var count int64
	if err := query.Count(&count).Error; err != nil {
		return slurmTriggerEvaluation{}, err
	}
	return slurmTriggerEvaluation{Matched: count > 0, Count: int(count), Message: fmt.Sprintf("%d released allocations match trigger", count)}, nil
}

func (h *Handler) slurmEvaluateRunEndTrigger(trigger *models.SlurmTrigger) (slurmTriggerEvaluation, error) {
	query := h.db.Model(&models.RunStep{}).Where("status IN ?", []models.Status{models.StatusSuccess, models.StatusFailed})
	if trigger.JobID != "" {
		query = query.Where("allocation_id IN ? OR id IN ?", slurmCSVValues(trigger.JobID), slurmCSVValues(trigger.JobID))
	}
	if trigger.UserID != "" {
		query = query.Where("user_id IN ?", slurmCSVValues(trigger.UserID))
	}
	if trigger.Partition != "" {
		query = query.Where("cluster IN ?", slurmCSVValues(trigger.Partition))
	}
	if trigger.Node != "" {
		query = query.Where("node IN ?", slurmCSVValues(trigger.Node))
	}
	var count int64
	if err := query.Count(&count).Error; err != nil {
		return slurmTriggerEvaluation{}, err
	}
	return slurmTriggerEvaluation{Matched: count > 0, Count: int(count), Message: fmt.Sprintf("%d completed run steps match trigger", count)}, nil
}

func (h *Handler) slurmEvaluateNodeTrigger(trigger *models.SlurmTrigger) (slurmTriggerEvaluation, error) {
	states := h.scheduler.GetClusterStates()
	count := 0
	for partition, cluster := range states {
		if trigger.Partition != "" && !containsCSVFold(trigger.Partition, partition) {
			continue
		}
		for nodeName, node := range cluster.Nodes {
			if trigger.Node != "" && !containsCSVFold(trigger.Node, nodeName) {
				continue
			}
			state := canonicalSlurmStateToken(slurmNodeStateFromState(node))
			switch trigger.Event {
			case "node_down":
				if state == "DOWN" {
					count++
				}
			case "node_drain":
				if state == "DRAIN" {
					count++
				}
			case "node_up":
				if state == "IDLE" || state == "ALLOCATED" || state == "MIXED" {
					count++
				}
			}
		}
	}
	return slurmTriggerEvaluation{Matched: count > 0, Count: count, Message: fmt.Sprintf("%d nodes match trigger", count)}, nil
}

func applySlurmTriggerJobFilters(query *gorm.DB, trigger *models.SlurmTrigger, idColumn string) (*gorm.DB, error) {
	if trigger.JobID != "" {
		selectors, err := parseSlurmJobSelectors(trigger.JobID)
		if err != nil {
			return nil, err
		}
		query = applySlurmJobSelectorQuery(query, idColumn, selectors)
	}
	if trigger.UserID != "" {
		query = query.Where("user_id IN ?", slurmCSVValues(trigger.UserID))
	}
	if trigger.Partition != "" {
		query = query.Where("cluster IN ?", slurmCSVValues(trigger.Partition))
	}
	if trigger.Node != "" {
		query = query.Where("node IN ?", slurmCSVValues(trigger.Node))
	}
	return query, nil
}

func applySlurmTriggerAccountingFilters(query *gorm.DB, trigger *models.SlurmTrigger) (*gorm.DB, error) {
	if trigger.JobID != "" {
		selectors, err := parseSlurmJobSelectors(trigger.JobID)
		if err != nil {
			return nil, err
		}
		query = applySlurmJobSelectorQuery(query, "submission_id", selectors)
	}
	if trigger.UserID != "" {
		query = query.Where("user_id IN ?", slurmCSVValues(trigger.UserID))
	}
	if trigger.Partition != "" {
		query = query.Where("cluster IN ?", slurmCSVValues(trigger.Partition))
	}
	if trigger.Node != "" {
		query = query.Where("node IN ?", slurmCSVValues(trigger.Node))
	}
	if trigger.State != "" {
		query = query.Where("state IN ?", slurmCSVValues(trigger.State))
	}
	return query, nil
}

type slurmCronScheduleField struct {
	any    bool
	values map[int]bool
}

func slurmCronFromRequest(req slurmCronRequest, existing *models.SlurmCronJob, now time.Time) (*models.SlurmCronJob, error) {
	entry := &models.SlurmCronJob{Enabled: true}
	if existing != nil {
		copied := *existing
		entry = &copied
	}

	id := strings.TrimSpace(firstNonEmpty(req.ID, req.EntryID, entry.ID))
	if id == "" {
		id = "cron-" + uuid.NewString()
	}
	entry.ID = id
	if strings.TrimSpace(req.Name) != "" {
		entry.Name = strings.TrimSpace(req.Name)
	}
	if strings.TrimSpace(req.Schedule) != "" {
		entry.Schedule = strings.TrimSpace(req.Schedule)
	}
	if strings.TrimSpace(entry.Schedule) == "" {
		return nil, fmt.Errorf("schedule is required")
	}
	if req.Enabled != nil {
		entry.Enabled = *req.Enabled
	}

	if batchJSON := strings.TrimSpace(string(req.Batch)); batchJSON != "" && batchJSON != "null" {
		var batchReq slurmBatchRequest
		if err := json.Unmarshal(req.Batch, &batchReq); err != nil {
			return nil, fmt.Errorf("invalid batch: %w", err)
		}
		normalizeSlurmBatchRequestAliases(&batchReq)
		if strings.TrimSpace(batchReq.UserID) == "" || strings.TrimSpace(batchReq.ProblemID) == "" {
			return nil, fmt.Errorf("batch.user_id and batch.problem_id are required")
		}
		encoded, err := json.Marshal(batchReq)
		if err != nil {
			return nil, fmt.Errorf("encode batch: %w", err)
		}
		entry.BatchJSON = string(encoded)
		entry.UserID = strings.TrimSpace(batchReq.UserID)
		entry.ProblemID = strings.TrimSpace(batchReq.ProblemID)
	} else if strings.TrimSpace(entry.BatchJSON) == "" {
		return nil, fmt.Errorf("batch is required")
	}

	switch {
	case req.NextRunAt != nil:
		nextRun := req.NextRunAt.Time
		entry.NextRunAt = &nextRun
	case req.NextRun != nil:
		nextRun := req.NextRun.Time
		entry.NextRunAt = &nextRun
	case existing == nil || strings.TrimSpace(req.Schedule) != "":
		nextRun, err := slurmCronNextRun(entry.Schedule, now)
		if err != nil {
			return nil, err
		}
		entry.NextRunAt = &nextRun
	}

	return entry, nil
}

func slurmCronRecord(entry *models.SlurmCronJob) map[string]interface{} {
	var batch interface{}
	if strings.TrimSpace(entry.BatchJSON) != "" {
		_ = json.Unmarshal([]byte(entry.BatchJSON), &batch)
	}
	return map[string]interface{}{
		"entry_id":    entry.ID,
		"id":          entry.ID,
		"name":        entry.Name,
		"schedule":    entry.Schedule,
		"enabled":     entry.Enabled,
		"user_id":     entry.UserID,
		"problem_id":  entry.ProblemID,
		"next_run_at": entry.NextRunAt,
		"next_run":    entry.NextRunAt,
		"last_run_at": entry.LastRunAt,
		"last_job_id": entry.LastJobID,
		"run_count":   entry.RunCount,
		"message":     entry.Message,
		"batch":       batch,
		"batch_json":  entry.BatchJSON,
		"created_at":  entry.CreatedAt,
		"updated_at":  entry.UpdatedAt,
	}
}

func slurmCronEvaluationRecord(entry *models.SlurmCronJob, submitted bool, response gin.H, err error) map[string]interface{} {
	record := slurmCronRecord(entry)
	record["submitted"] = submitted
	if response != nil {
		record["response"] = response
		if jobID := fmt.Sprint(response["job_id"]); jobID != "" && jobID != "<nil>" {
			record["job_id"] = jobID
			record["submission_id"] = jobID
		}
	}
	if _, ok := record["job_id"]; !ok && entry.LastJobID != "" {
		record["job_id"] = entry.LastJobID
		record["submission_id"] = entry.LastJobID
	}
	if err != nil {
		record["error"] = err.Error()
	}
	return record
}

func slurmCronNextRun(schedule string, after time.Time) (time.Time, error) {
	schedule = strings.TrimSpace(schedule)
	if schedule == "" {
		return time.Time{}, fmt.Errorf("cron schedule is required")
	}

	lower := strings.ToLower(schedule)
	switch lower {
	case "@hourly":
		return after.Truncate(time.Hour).Add(time.Hour), nil
	case "@daily", "@midnight":
		return slurmNextLocalMidnight(after), nil
	case "@weekly":
		next := slurmNextLocalMidnight(after)
		for next.Weekday() != time.Sunday {
			next = next.AddDate(0, 0, 1)
		}
		return next, nil
	case "@monthly":
		year, month, _ := after.Date()
		return time.Date(year, month+1, 1, 0, 0, 0, 0, after.Location()), nil
	case "@yearly", "@annually":
		year, _, _ := after.Date()
		return time.Date(year+1, time.January, 1, 0, 0, 0, 0, after.Location()), nil
	}
	if strings.HasPrefix(lower, "@every ") {
		return slurmCronNextEvery(strings.TrimSpace(schedule[len("@every "):]), after)
	}
	if strings.HasPrefix(lower, "every ") {
		return slurmCronNextEvery(strings.TrimSpace(schedule[len("every "):]), after)
	}

	fields := strings.Fields(schedule)
	if len(fields) != 5 {
		return time.Time{}, fmt.Errorf("cron schedule must be @hourly, @daily, @weekly, @monthly, @every <duration>, or five fields")
	}
	minute, err := slurmParseCronField(fields[0], 0, 59, nil, nil)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid minute field: %w", err)
	}
	hour, err := slurmParseCronField(fields[1], 0, 23, nil, nil)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid hour field: %w", err)
	}
	dayOfMonth, err := slurmParseCronField(fields[2], 1, 31, nil, nil)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid day-of-month field: %w", err)
	}
	month, err := slurmParseCronField(fields[3], 1, 12, slurmCronMonthNames(), nil)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid month field: %w", err)
	}
	dayOfWeek, err := slurmParseCronField(fields[4], 0, 7, slurmCronDayNames(), func(value int) int {
		if value == 7 {
			return 0
		}
		return value
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid day-of-week field: %w", err)
	}

	start := after.Truncate(time.Minute).Add(time.Minute)
	deadline := start.AddDate(1, 0, 1)
	for candidate := start; !candidate.After(deadline); candidate = candidate.Add(time.Minute) {
		if slurmCronMatches(candidate, minute, hour, dayOfMonth, month, dayOfWeek) {
			return candidate, nil
		}
	}
	return time.Time{}, fmt.Errorf("cron schedule has no run time within one year")
}

func slurmNextLocalMidnight(after time.Time) time.Time {
	year, month, day := after.Date()
	return time.Date(year, month, day+1, 0, 0, 0, 0, after.Location())
}

func slurmCronNextEvery(value string, after time.Time) (time.Time, error) {
	duration, err := time.ParseDuration(value)
	if err != nil {
		seconds, parseErr := parseSlurmTimeLimit(value)
		if parseErr != nil {
			return time.Time{}, fmt.Errorf("invalid @every duration %q", value)
		}
		duration = time.Duration(seconds) * time.Second
	}
	if duration <= 0 {
		return time.Time{}, fmt.Errorf("@every duration must be positive")
	}
	return after.Add(duration), nil
}

func slurmCronMatches(candidate time.Time, minute, hour, dayOfMonth, month, dayOfWeek slurmCronScheduleField) bool {
	if !minute.matches(candidate.Minute()) || !hour.matches(candidate.Hour()) || !month.matches(int(candidate.Month())) {
		return false
	}
	monthDayMatch := dayOfMonth.matches(candidate.Day())
	weekDayMatch := dayOfWeek.matches(int(candidate.Weekday()))
	switch {
	case dayOfMonth.any && dayOfWeek.any:
		return true
	case dayOfMonth.any:
		return weekDayMatch
	case dayOfWeek.any:
		return monthDayMatch
	default:
		return monthDayMatch || weekDayMatch
	}
}

func (field slurmCronScheduleField) matches(value int) bool {
	if field.any {
		return true
	}
	return field.values[value]
}

func slurmParseCronField(raw string, minValue, maxValue int, names map[string]int, normalize func(int) int) (slurmCronScheduleField, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return slurmCronScheduleField{}, fmt.Errorf("empty field")
	}
	if raw == "*" || raw == "?" {
		return slurmCronScheduleField{any: true}, nil
	}

	field := slurmCronScheduleField{values: make(map[int]bool)}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		step := 1
		if base, stepText, ok := strings.Cut(part, "/"); ok {
			part = strings.TrimSpace(base)
			parsedStep, err := strconv.Atoi(strings.TrimSpace(stepText))
			if err != nil || parsedStep <= 0 {
				return slurmCronScheduleField{}, fmt.Errorf("invalid step %q", stepText)
			}
			step = parsedStep
		}

		start, end, err := slurmCronFieldRange(part, minValue, maxValue, names)
		if err != nil {
			return slurmCronScheduleField{}, err
		}
		if start > end {
			return slurmCronScheduleField{}, fmt.Errorf("range start %d is greater than end %d", start, end)
		}
		for value := start; value <= end; value += step {
			normalized := value
			if normalize != nil {
				normalized = normalize(value)
			}
			field.values[normalized] = true
		}
	}
	if len(field.values) == 0 {
		return slurmCronScheduleField{}, fmt.Errorf("field has no values")
	}
	return field, nil
}

func slurmCronFieldRange(raw string, minValue, maxValue int, names map[string]int) (int, int, error) {
	raw = strings.TrimSpace(raw)
	switch raw {
	case "*", "?":
		return minValue, maxValue, nil
	case "":
		return 0, 0, fmt.Errorf("empty value")
	}
	if startText, endText, ok := strings.Cut(raw, "-"); ok {
		start, err := slurmCronFieldValue(startText, minValue, maxValue, names)
		if err != nil {
			return 0, 0, err
		}
		end, err := slurmCronFieldValue(endText, minValue, maxValue, names)
		if err != nil {
			return 0, 0, err
		}
		return start, end, nil
	}
	value, err := slurmCronFieldValue(raw, minValue, maxValue, names)
	if err != nil {
		return 0, 0, err
	}
	return value, value, nil
}

func slurmCronFieldValue(raw string, minValue, maxValue int, names map[string]int) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty value")
	}
	if names != nil {
		key := strings.ToUpper(raw)
		if len(key) > 3 {
			key = key[:3]
		}
		if value, ok := names[key]; ok {
			return value, nil
		}
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", raw)
	}
	if value < minValue || value > maxValue {
		return 0, fmt.Errorf("value %d outside %d-%d", value, minValue, maxValue)
	}
	return value, nil
}

func slurmCronMonthNames() map[string]int {
	return map[string]int{
		"JAN": 1,
		"FEB": 2,
		"MAR": 3,
		"APR": 4,
		"MAY": 5,
		"JUN": 6,
		"JUL": 7,
		"AUG": 8,
		"SEP": 9,
		"OCT": 10,
		"NOV": 11,
		"DEC": 12,
	}
}

func slurmCronDayNames() map[string]int {
	return map[string]int{
		"SUN": 0,
		"MON": 1,
		"TUE": 2,
		"WED": 3,
		"THU": 4,
		"FRI": 5,
		"SAT": 6,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func slurmCSVValues(values ...string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0)
	for _, value := range values {
		for _, part := range slurmListValues(value) {
			item := strings.TrimSpace(part)
			if item == "" || seen[item] {
				continue
			}
			seen[item] = true
			result = append(result, item)
		}
	}
	return result
}

func slurmListValues(value string) []string {
	parts := make([]string, 0)
	var current strings.Builder
	bracketDepth := 0
	for _, r := range value {
		switch r {
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		}
		if bracketDepth == 0 && (r == ',' || r == ';' || r == ' ') {
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
			continue
		}
		current.WriteRune(r)
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

func canonicalSlurmReportGroup(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "account", "accounts":
		return "account", nil
	case "user", "users", "user_id":
		return "user", nil
	case "partition", "partitions", "cluster", "clusters":
		return "partition", nil
	case "qos":
		return "qos", nil
	case "job", "jobs", "job_id", "submission", "submission_id":
		return "job", nil
	default:
		return "", fmt.Errorf("unsupported sreport group_by %q", value)
	}
}

func slurmReportGroupKey(record models.AccountingRecord, groupBy string) string {
	switch groupBy {
	case "user":
		return firstNonEmpty(record.UserID, "(none)")
	case "partition":
		return firstNonEmpty(record.Cluster, "(none)")
	case "qos":
		return firstNonEmpty(record.QOS, "(none)")
	case "job":
		return firstNonEmpty(record.SubmissionID, record.ContainerID, "(none)")
	default:
		return firstNonEmpty(record.Account, "(none)")
	}
}

func slurmReportGroupField(groupBy string) string {
	switch groupBy {
	case "user":
		return "user_id"
	case "partition":
		return "partition"
	case "qos":
		return "qos"
	case "job":
		return "job_id"
	default:
		return "account"
	}
}

func slurmReportUsageEvents() []string {
	return []string{
		database.AccountEventCompleted,
		database.AccountEventFailed,
		database.AccountEventPreempted,
		database.AccountEventInterrupted,
		database.AccountEventAllocationReleased,
		database.AccountEventRunCompleted,
		database.AccountEventRunFailed,
	}
}

func (h *Handler) slurmEfficiencyRecord(jobID, stepID string) (map[string]interface{}, error) {
	parsedStepID := slurmAttachStepID(stepID)
	var records []models.AccountingRecord
	query := h.db.Where("submission_id = ?", jobID).Order("created_at asc")
	if parsedStepID != "" {
		query = query.Where("step_name = ? OR container_id = ?", parsedStepID, parsedStepID)
	}
	if err := query.Find(&records).Error; err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("accounting records for job %q not found", jobID)
	}

	summary := slurmEfficiencySummaryFromAccounting(jobID, parsedStepID, records)
	usage := h.slurmEfficiencyUsageFromRunSteps(jobID, parsedStepID)
	allocatedCPUSeconds := float64(summary.ElapsedSeconds * int64(summary.AllocCPU))
	cpuEfficiency := interface{}(nil)
	cpuEfficiencyAvailable := usage.CPUSeconds > 0 && allocatedCPUSeconds > 0
	if cpuEfficiencyAvailable {
		cpuEfficiency = usage.CPUSeconds / allocatedCPUSeconds * 100
	}
	maxRSSMB := float64(0)
	if usage.MaxRSS > 0 {
		maxRSSMB = float64(usage.MaxRSS) / 1024 / 1024
	}
	memoryEfficiency := interface{}(nil)
	memoryEfficiencyAvailable := maxRSSMB > 0 && summary.AllocMemory > 0
	if memoryEfficiencyAvailable {
		memoryEfficiency = maxRSSMB / float64(summary.AllocMemory) * 100
	}
	efficiencyAvailable := cpuEfficiencyAvailable || memoryEfficiencyAvailable
	message := ""
	if !efficiencyAvailable {
		message = "runtime usage samples are not available for this job"
	}

	return map[string]interface{}{
		"job_id":                      jobID,
		"step_id":                     parsedStepID,
		"job_name":                    summary.JobName,
		"problem_id":                  summary.ProblemID,
		"user_id":                     summary.UserID,
		"partition":                   summary.Cluster,
		"node":                        summary.Node,
		"account":                     summary.Account,
		"qos":                         summary.QOS,
		"state":                       summary.State,
		"native_state":                summary.NativeState,
		"event":                       summary.Event,
		"reason":                      summary.Reason,
		"start_time":                  summary.StartTime,
		"end_time":                    summary.EndTime,
		"elapsed_seconds":             summary.ElapsedSeconds,
		"alloc_cpus":                  summary.AllocCPU,
		"alloc_mem":                   summary.AllocMemory,
		"allocated_cpu_seconds":       allocatedCPUSeconds,
		"cpu_used_seconds":            usage.CPUSeconds,
		"cpu_efficiency":              cpuEfficiency,
		"cpu_efficiency_available":    cpuEfficiencyAvailable,
		"max_rss":                     usage.MaxRSS,
		"max_rss_mb":                  maxRSSMB,
		"memory_efficiency":           memoryEfficiency,
		"memory_efficiency_available": memoryEfficiencyAvailable,
		"billing_units":               summary.BillingUnits,
		"record_count":                len(records),
		"step_count":                  usage.StepCount,
		"usage_source":                usage.Source,
		"efficiency_available":        efficiencyAvailable,
		"message":                     message,
	}, nil
}

type slurmEfficiencySummary struct {
	JobName        string
	ProblemID      string
	UserID         string
	Cluster        string
	Node           string
	Account        string
	QOS            string
	State          string
	NativeState    models.Status
	Event          string
	Reason         string
	StartTime      *time.Time
	EndTime        *time.Time
	ElapsedSeconds int64
	AllocCPU       int
	AllocMemory    int64
	BillingUnits   float64
}

type slurmEfficiencyUsage struct {
	CPUSeconds float64
	MaxRSS     int64
	StepCount  int
	Source     string
}

func slurmEfficiencySummaryFromAccounting(jobID, stepID string, records []models.AccountingRecord) slurmEfficiencySummary {
	summary := slurmEfficiencySummary{Event: records[len(records)-1].Event}
	var startTime time.Time
	var endTime time.Time
	for _, record := range records {
		if summary.JobName == "" {
			summary.JobName = slurmAccountingJobName(record)
		}
		summary.ProblemID = firstNonEmpty(summary.ProblemID, record.ProblemID)
		summary.UserID = firstNonEmpty(summary.UserID, record.UserID)
		summary.Cluster = firstNonEmpty(summary.Cluster, record.Cluster)
		summary.Node = firstNonEmpty(summary.Node, record.Node)
		summary.Account = firstNonEmpty(summary.Account, record.Account)
		summary.QOS = firstNonEmpty(summary.QOS, record.QOS)
		if record.CPU > summary.AllocCPU {
			summary.AllocCPU = record.CPU
		}
		if record.Memory > summary.AllocMemory {
			summary.AllocMemory = record.Memory
		}
		if record.BillingUnits > summary.BillingUnits {
			summary.BillingUnits = record.BillingUnits
		}
		if slurmEfficiencyStartEvent(record.Event) && (startTime.IsZero() || record.CreatedAt.Before(startTime)) {
			startTime = record.CreatedAt
		}
		if slurmEfficiencyEndEvent(record.Event) && (endTime.IsZero() || record.CreatedAt.After(endTime)) {
			endTime = record.CreatedAt
			summary.Event = record.Event
			summary.NativeState = record.State
			summary.Reason = record.Reason
		}
	}
	if startTime.IsZero() {
		startTime = records[0].CreatedAt
	}
	if endTime.IsZero() {
		now := time.Now()
		endTime = now
		latest := records[len(records)-1]
		summary.Event = latest.Event
		summary.NativeState = latest.State
		summary.Reason = latest.Reason
	}
	summary.StartTime = &startTime
	summary.EndTime = &endTime
	if endTime.After(startTime) {
		summary.ElapsedSeconds = int64(endTime.Sub(startTime).Seconds())
	}
	state, reason := models.DeriveSlurmJobState(summary.NativeState, false, summary.Reason)
	switch summary.Event {
	case database.AccountEventInterrupted:
		state = models.SlurmStateCancelled
		reason = "Cancelled"
	case database.AccountEventPreempted:
		state = models.SlurmStatePreempted
		reason = "Preempted"
	}
	if state == "" {
		state = models.SlurmStatePending
	}
	summary.State = state
	summary.Reason = reason
	if summary.JobName == "" {
		summary.JobName = firstNonEmpty(stepID, jobID)
	}
	return summary
}

func slurmEfficiencyStartEvent(event string) bool {
	switch event {
	case database.AccountEventStarted, database.AccountEventRunStarted, database.AccountEventAllocated, database.AccountEventContainerStarted:
		return true
	default:
		return false
	}
}

func slurmEfficiencyEndEvent(event string) bool {
	switch event {
	case database.AccountEventCompleted, database.AccountEventFailed, database.AccountEventInterrupted, database.AccountEventPreempted, database.AccountEventAllocationReleased, database.AccountEventRunCompleted, database.AccountEventRunFailed, database.AccountEventContainerFinished:
		return true
	default:
		return false
	}
}

func (h *Handler) slurmEfficiencyUsageFromRunSteps(jobID, stepID string) slurmEfficiencyUsage {
	query := h.db.Model(&models.RunStep{}).Where("allocation_id = ?", jobID)
	if stepID != "" {
		query = query.Where("id = ?", stepID)
	}
	var steps []models.RunStep
	if err := query.Find(&steps).Error; err != nil || len(steps) == 0 {
		return slurmEfficiencyUsage{Source: "accounting"}
	}
	usage := slurmEfficiencyUsage{StepCount: len(steps), Source: "srun_steps"}
	for _, step := range steps {
		usage.CPUSeconds += step.AveCPU
		if step.MaxRSS > usage.MaxRSS {
			usage.MaxRSS = step.MaxRSS
		}
	}
	return usage
}

func slurmBroadcastFiles(req slurmBroadcastRequest) (map[string][]byte, error) {
	files := make(map[string][]byte)
	singleDestination := firstNonEmpty(req.Path, req.Destination, req.DestPath)
	if singleDestination != "" {
		switch {
		case req.Content != nil:
			files[singleDestination] = []byte(*req.Content)
		case req.ContentBase64 != nil:
			data, err := base64.StdEncoding.DecodeString(*req.ContentBase64)
			if err != nil {
				return nil, fmt.Errorf("invalid content_base64: %w", err)
			}
			files[singleDestination] = data
		default:
			return nil, fmt.Errorf("content or content_base64 is required for path")
		}
	}
	for destination, content := range req.Files {
		files[destination] = []byte(content)
	}
	for destination, encoded := range req.FilesBase64 {
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("invalid files_base64 value for %q: %w", destination, err)
		}
		files[destination] = data
	}
	return files, nil
}

func slurmAttachStepID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if before, after, ok := strings.Cut(value, "."); ok {
		before = strings.TrimSpace(before)
		after = strings.TrimSpace(after)
		if before != "" && after != "" {
			return after
		}
	}
	return value
}

func slurmCSVInts(value string) ([]int, error) {
	values := slurmCSVValues(value)
	out := make([]int, 0, len(values))
	for _, item := range values {
		parsed, err := strconv.Atoi(item)
		if err != nil {
			return nil, fmt.Errorf("invalid integer value %q", item)
		}
		out = append(out, parsed)
	}
	return out, nil
}

type slurmArrayTaskSelector struct {
	ArrayJobID string
	TaskIDs    []int
}

type slurmJobSelectorSet struct {
	JobIDs     []string
	ArrayTasks []slurmArrayTaskSelector
}

func parseSlurmJobSelectors(values ...string) (slurmJobSelectorSet, error) {
	selectors := slurmJobSelectorSet{}
	seenJobIDs := make(map[string]bool)
	for _, item := range slurmCSVValues(values...) {
		arraySelector, ok, err := parseSlurmArrayTaskJobID(item)
		if err != nil {
			return slurmJobSelectorSet{}, err
		}
		if ok {
			selectors.addArrayTaskSelector(arraySelector)
			continue
		}
		if !seenJobIDs[item] {
			seenJobIDs[item] = true
			selectors.JobIDs = append(selectors.JobIDs, item)
		}
	}
	return selectors, nil
}

func parseSlurmArrayTaskJobID(value string) (slurmArrayTaskSelector, bool, error) {
	value = strings.TrimSpace(value)
	idx := strings.LastIndex(value, "_")
	if idx <= 0 || idx >= len(value)-1 {
		return slurmArrayTaskSelector{}, false, nil
	}

	taskSpec := strings.TrimSpace(value[idx+1:])
	if strings.HasPrefix(taskSpec, "[") && strings.HasSuffix(taskSpec, "]") {
		taskSpec = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(taskSpec, "["), "]"))
	}
	arrayJobID := strings.TrimSpace(value[:idx])
	if arrayJobID == "" || taskSpec == "" {
		return slurmArrayTaskSelector{}, false, nil
	}

	array, err := judger.ParseJobArray(taskSpec)
	if err != nil {
		if looksLikeSlurmArrayTaskSpec(taskSpec) {
			return slurmArrayTaskSelector{}, true, fmt.Errorf("invalid array task selector %q: %w", value, err)
		}
		return slurmArrayTaskSelector{}, false, nil
	}
	return slurmArrayTaskSelector{ArrayJobID: arrayJobID, TaskIDs: array.TaskIDs}, true, nil
}

func looksLikeSlurmArrayTaskSpec(value string) bool {
	hasDigit := false
	for _, r := range value {
		switch {
		case r >= '0' && r <= '9':
			hasDigit = true
		case r == '-' || r == ':' || r == ',' || r == '%' || r == '[' || r == ']':
		default:
			return false
		}
	}
	return hasDigit
}

func (selectors slurmJobSelectorSet) Has() bool {
	return len(selectors.JobIDs) > 0 || len(selectors.ArrayTasks) > 0
}

func (selectors slurmJobSelectorSet) Matches(jobID, arrayJobID string, arrayTaskID int) bool {
	for _, candidate := range selectors.JobIDs {
		if candidate == jobID {
			return true
		}
	}
	for _, selector := range selectors.ArrayTasks {
		if selector.ArrayJobID != arrayJobID {
			continue
		}
		for _, taskID := range selector.TaskIDs {
			if taskID == arrayTaskID {
				return true
			}
		}
	}
	return false
}

func (selectors *slurmJobSelectorSet) addArrayTaskSelector(selector slurmArrayTaskSelector) {
	for i := range selectors.ArrayTasks {
		if selectors.ArrayTasks[i].ArrayJobID != selector.ArrayJobID {
			continue
		}
		seen := make(map[int]bool, len(selectors.ArrayTasks[i].TaskIDs))
		for _, taskID := range selectors.ArrayTasks[i].TaskIDs {
			seen[taskID] = true
		}
		for _, taskID := range selector.TaskIDs {
			if !seen[taskID] {
				selectors.ArrayTasks[i].TaskIDs = append(selectors.ArrayTasks[i].TaskIDs, taskID)
			}
		}
		sort.Ints(selectors.ArrayTasks[i].TaskIDs)
		return
	}
	selectors.ArrayTasks = append(selectors.ArrayTasks, selector)
}

func applySlurmJobSelectorQuery(query *gorm.DB, idColumn string, selectors slurmJobSelectorSet) *gorm.DB {
	if !selectors.Has() {
		return query
	}
	clauses := make([]string, 0, len(selectors.ArrayTasks)+1)
	args := make([]interface{}, 0, len(selectors.ArrayTasks)*2+1)
	if len(selectors.JobIDs) > 0 {
		clauses = append(clauses, idColumn+" IN ?")
		args = append(args, selectors.JobIDs)
	}
	for _, selector := range selectors.ArrayTasks {
		if selector.ArrayJobID == "" || len(selector.TaskIDs) == 0 {
			continue
		}
		clauses = append(clauses, "(array_job_id = ? AND array_task_id IN ?)")
		args = append(args, selector.ArrayJobID, selector.TaskIDs)
	}
	if len(clauses) == 0 {
		return query
	}
	return query.Where("("+strings.Join(clauses, " OR ")+")", args...)
}

func applySlurmArrayWideJobSelectorQuery(query *gorm.DB, idColumn string, selectors slurmJobSelectorSet) *gorm.DB {
	if !selectors.Has() {
		return query
	}
	clauses := make([]string, 0, len(selectors.ArrayTasks)+2)
	args := make([]interface{}, 0, len(selectors.ArrayTasks)*2+2)
	if len(selectors.JobIDs) > 0 {
		clauses = append(clauses, idColumn+" IN ?")
		args = append(args, selectors.JobIDs)
		clauses = append(clauses, "array_job_id IN ?")
		args = append(args, selectors.JobIDs)
	}
	for _, selector := range selectors.ArrayTasks {
		if selector.ArrayJobID == "" || len(selector.TaskIDs) == 0 {
			continue
		}
		clauses = append(clauses, "(array_job_id = ? AND array_task_id IN ?)")
		args = append(args, selector.ArrayJobID, selector.TaskIDs)
	}
	if len(clauses) == 0 {
		return query
	}
	return query.Where("("+strings.Join(clauses, " OR ")+")", args...)
}

func containsCSVFold(values, value string) bool {
	if strings.TrimSpace(values) == "" {
		return true
	}
	for _, part := range slurmCSVValues(values) {
		if strings.EqualFold(part, value) {
			return true
		}
	}
	return false
}

func stringInFoldList(value string, values []string) bool {
	for _, candidate := range values {
		if strings.EqualFold(candidate, value) {
			return true
		}
	}
	return false
}

func slurmStateQuery(c *gin.Context) string {
	return firstQuery(c, "state", "states", "t")
}

func slurmStateFilterMatches(filter string, states ...string) bool {
	filterTokens := slurmCSVValues(filter)
	if len(filterTokens) == 0 {
		return true
	}

	stateSet := make(map[string]bool)
	for _, state := range states {
		token := canonicalSlurmStateToken(state)
		if token == "" {
			continue
		}
		stateSet[token] = true
	}
	if len(stateSet) == 0 {
		return false
	}

	for _, value := range filterTokens {
		if stateSet[canonicalSlurmStateToken(value)] {
			return true
		}
	}
	return false
}

func canonicalSlurmStateToken(state string) string {
	token := strings.ToUpper(strings.TrimSpace(state))
	token = strings.ReplaceAll(token, "-", "_")
	token = strings.ReplaceAll(token, " ", "_")
	switch token {
	case "":
		return ""
	case "PD", "PEND", "PENDING", "QUEUED":
		return models.SlurmStatePending
	case "R", "RUN", "RUNNING":
		return models.SlurmStateRunning
	case "S", "SUSP", "SUSPENDED", "STOPPED":
		return models.SlurmStateSuspended
	case "CD", "COMPLETE", "COMPLETED", "SUCCESS":
		return models.SlurmStateCompleted
	case "CA", "CANCELLED", "CANCELED":
		return models.SlurmStateCancelled
	case "F", "FAIL", "FAILED":
		return models.SlurmStateFailed
	case "DL", "DEADLINE":
		return models.SlurmStateDeadline
	case "NF", "NODEFAIL", "NODE_FAIL":
		return models.SlurmStateNodeFail
	case "OOM", "OUTOFMEMORY", "OUT_OF_MEMORY":
		return models.SlurmStateOOM
	case "PR", "PREEMPT", "PREEMPTED":
		return models.SlurmStatePreempted
	case "TO", "TIMEOUT", "TIMEDOUT":
		return models.SlurmStateTimeout
	case "UNK", "UNKNOWN":
		return models.SlurmStateUnknown
	case "I", "IDLE":
		return "IDLE"
	case "ALLOC", "ALLOCATED":
		return "ALLOCATED"
	case "MIX", "MIXED":
		return "MIXED"
	case "DRAIN", "DRAINED", "DRAINING":
		return "DRAIN"
	case "DOWN":
		return "DOWN"
	case "INACTIVE":
		return "INACTIVE"
	case "UP":
		return "UP"
	default:
		return token
	}
}

func normalizeSlurmNodeState(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "idle", "resume", "undrain":
		return "idle"
	case "drain", "drained":
		return "drain"
	case "down":
		return "down"
	case "inactive":
		return "inactive"
	default:
		return strings.ToLower(strings.TrimSpace(state))
	}
}

func normalizeSlurmPartitionState(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "":
		return ""
	case "up", "idle", "mixed", "alloc", "allocated", "resume":
		return "up"
	case "down":
		return "down"
	case "drain", "drained", "draining":
		return "drain"
	case "inactive":
		return "inactive"
	default:
		return strings.ToLower(strings.TrimSpace(state))
	}
}

func normalizeSlurmBatchRequestAliases(req *slurmBatchRequest) {
	if req == nil {
		return
	}
	if strings.TrimSpace(req.UserID) == "" {
		req.UserID = req.User
	}
	if strings.TrimSpace(req.JobName) == "" {
		req.JobName = req.Name
	}
	if strings.TrimSpace(req.WorkDir) == "" {
		req.WorkDir = req.Chdir
	}
	if strings.TrimSpace(req.StdinPath) == "" {
		req.StdinPath = req.Input
	}
	if strings.TrimSpace(req.StdoutPath) == "" {
		req.StdoutPath = req.Output
	}
	if strings.TrimSpace(req.StderrPath) == "" {
		req.StderrPath = req.ErrorPath
	}
	if req.Memory == nil {
		req.Memory = req.Mem
	}
	if req.MemoryPerCPU == nil {
		req.MemoryPerCPU = req.MemPerCPU
	}
	if req.BeginTime == nil {
		req.BeginTime = firstSlurmDateTime(req.Begin, req.StartTime)
	}
	if req.TimeLimit == nil {
		req.TimeLimit = req.Time
	}
	if strings.TrimSpace(req.Partition) == "" {
		req.Partition = req.Cluster
	}
	if strings.TrimSpace(req.NodeList) == "" {
		req.NodeList = firstNonEmpty(req.Nodelist, req.NodesList)
	}
	if strings.TrimSpace(req.ExcludeNodes) == "" {
		req.ExcludeNodes = req.Exclude
	}
}

func applySlurmBatchWrap(req *slurmBatchRequest) {
	if req == nil || strings.TrimSpace(req.Script) != "" || strings.TrimSpace(req.Wrap) == "" {
		return
	}
	req.Script = "#!/bin/sh\n" + req.Wrap
	if !strings.HasSuffix(req.Script, "\n") {
		req.Script += "\n"
	}
}

func applySlurmBatchIODefaults(req *slurmBatchRequest) {
	if req == nil {
		return
	}
	if strings.TrimSpace(req.StdoutPath) == "" {
		req.StdoutPath = "slurm-%j.out"
	}
}

func applySlurmBatchEnvironment(req *slurmBatchRequest) error {
	if req == nil {
		return nil
	}
	env, err := parseSlurmExportEnvironment(req.Export, req.Environment)
	if err != nil {
		return err
	}
	req.Export = strings.TrimSpace(req.Export)
	req.Environment = env
	return nil
}

func parseSlurmExportEnvironment(export string, explicit models.JSONMap) (models.JSONMap, error) {
	env := models.JSONMap{}
	for _, token := range strings.Split(export, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		upper := strings.ToUpper(token)
		switch upper {
		case "ALL", "NONE", "NIL":
			continue
		}
		name, value, ok := strings.Cut(token, "=")
		if !ok {
			name = token
			value = ""
		}
		name = strings.TrimSpace(name)
		if !judger.IsEnvironmentName(name) {
			return nil, fmt.Errorf("invalid export variable name %q", name)
		}
		env[name] = value
	}
	for name, value := range explicit {
		name = strings.TrimSpace(name)
		if !judger.IsEnvironmentName(name) {
			return nil, fmt.Errorf("invalid environment variable name %q", name)
		}
		env[name] = fmt.Sprint(value)
	}
	if len(env) == 0 {
		return nil, nil
	}
	return env, nil
}

func applySlurmBatchResourceShape(req *slurmBatchRequest) {
	if req == nil {
		return
	}
	if req.NTasks == nil && req.Tasks != nil {
		req.NTasks = req.Tasks
	}

	totalCPU := 0
	tasks := 1
	if req.NTasks != nil && *req.NTasks > 0 {
		tasks = *req.NTasks
		totalCPU = tasks
	}
	if req.CPUsPerTask != nil && *req.CPUsPerTask > 0 {
		totalCPU = tasks * *req.CPUsPerTask
	}
	if req.CPU == nil && totalCPU > 0 {
		req.CPU = &totalCPU
	}
	if req.Memory == nil && req.MemoryPerCPU != nil && req.MemoryPerCPU.Int64() > 0 {
		cpus := totalCPU
		if req.CPU != nil && *req.CPU > 0 {
			cpus = *req.CPU
		}
		if cpus <= 0 {
			cpus = 1
		}
		memory := slurmMemoryMB(req.MemoryPerCPU.Int64() * int64(cpus))
		req.Memory = &memory
	}
}

func applySlurmScriptDirectives(req *slurmBatchRequest) error {
	if req == nil || strings.TrimSpace(req.Script) == "" {
		return nil
	}
	directives, err := parseSlurmScriptDirectives(req.Script)
	if err != nil {
		return err
	}
	for _, directive := range directives {
		if err := applySlurmDirective(req, directive.option, directive.value); err != nil {
			return err
		}
	}
	return nil
}

type slurmDirective struct {
	option string
	value  string
}

func parseSlurmScriptDirectives(script string) ([]slurmDirective, error) {
	lines := strings.Split(script, "\n")
	directives := make([]slurmDirective, 0)
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#!") {
			continue
		}
		if !strings.HasPrefix(trimmed, "#") {
			break
		}
		if !strings.HasPrefix(trimmed, "#SBATCH") {
			continue
		}
		body := strings.TrimSpace(strings.TrimPrefix(trimmed, "#SBATCH"))
		fields, err := slurmDirectiveFields(body)
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(fields); i++ {
			option := fields[i]
			value := ""
			if shortOption, shortValue, ok := splitSlurmCompactShortOption(option); ok {
				option = shortOption
				value = shortValue
			} else if before, after, ok := strings.Cut(option, "="); ok {
				option = before
				value = after
			} else if slurmDirectiveNeedsValue(option) {
				if i+1 >= len(fields) {
					return nil, fmt.Errorf("missing value for #SBATCH %s", option)
				}
				i++
				value = fields[i]
			}
			directives = append(directives, slurmDirective{option: option, value: value})
		}
	}
	return directives, nil
}

func splitSlurmCompactShortOption(token string) (string, string, bool) {
	if len(token) <= 2 || !strings.HasPrefix(token, "-") || strings.HasPrefix(token, "--") {
		return "", "", false
	}
	option := token[:2]
	if !slurmCompactShortOptionNeedsValue(option) {
		return "", "", false
	}
	return option, token[2:], true
}

func slurmCompactShortOptionNeedsValue(option string) bool {
	switch option {
	case "-p", "-A", "-n", "-N", "-c", "-t", "-d", "-w", "-x", "-C", "-a", "-J", "-D", "-i", "-o", "-e":
		return true
	default:
		return false
	}
}

func slurmDirectiveFields(value string) ([]string, error) {
	fields := make([]string, 0)
	var current strings.Builder
	quote := rune(0)
	escaped := false
	for _, r := range value {
		if escaped {
			current.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
				continue
			}
			current.WriteRune(r)
			continue
		}
		if r == '\'' || r == '"' {
			quote = r
			continue
		}
		if r == ' ' || r == '\t' {
			if current.Len() > 0 {
				fields = append(fields, current.String())
				current.Reset()
			}
			continue
		}
		current.WriteRune(r)
	}
	if escaped {
		current.WriteRune('\\')
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote in #SBATCH directive")
	}
	if current.Len() > 0 {
		fields = append(fields, current.String())
	}
	return fields, nil
}

func slurmDirectiveNeedsValue(option string) bool {
	switch option {
	case "--hold", "--exclusive", "--requeue", "--no-requeue", "--kill-on-invalid-dep", "--no-kill":
		return false
	default:
		return strings.HasPrefix(option, "-")
	}
}

func applySlurmDirective(req *slurmBatchRequest, option, value string) error {
	option = strings.TrimSpace(option)
	value = strings.TrimSpace(value)
	switch option {
	case "-p", "--partition":
		if req.Partition == "" {
			req.Partition = value
		}
	case "-A", "--account":
		if req.Account == "" {
			req.Account = value
		}
	case "--qos":
		if req.QOS == "" {
			req.QOS = value
		}
	case "--priority":
		if req.Priority == nil {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid #SBATCH priority %q: %w", value, err)
			}
			req.Priority = &parsed
		}
	case "--nice":
		if req.Nice == nil {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid #SBATCH nice %q: %w", value, err)
			}
			req.Nice = &parsed
		}
	case "-n", "--ntasks":
		if req.NTasks == nil {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid #SBATCH ntasks %q: %w", value, err)
			}
			req.NTasks = &parsed
		}
	case "-N", "--nodes":
		if req.Nodes == nil {
			parsed, err := parseSlurmNodeCount(value)
			if err != nil {
				return fmt.Errorf("invalid #SBATCH nodes %q: %w", value, err)
			}
			req.Nodes = &parsed
		}
	case "-c", "--cpus-per-task":
		if req.CPUsPerTask == nil {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid #SBATCH cpus-per-task %q: %w", value, err)
			}
			req.CPUsPerTask = &parsed
		}
	case "--cpus-per-job":
		if req.CPU == nil {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid #SBATCH cpus %q: %w", value, err)
			}
			req.CPU = &parsed
		}
	case "--mem":
		if req.Memory == nil {
			parsed, err := parseSlurmMemoryMB(value)
			if err != nil {
				return fmt.Errorf("invalid #SBATCH memory %q: %w", value, err)
			}
			memory := slurmMemoryMB(parsed)
			req.Memory = &memory
		}
	case "--mem-per-cpu":
		if req.Memory == nil && req.MemoryPerCPU == nil {
			parsed, err := parseSlurmMemoryMB(value)
			if err != nil {
				return fmt.Errorf("invalid #SBATCH memory %q: %w", value, err)
			}
			memory := slurmMemoryMB(parsed)
			req.MemoryPerCPU = &memory
		}
	case "--hold":
		if req.Hold == nil {
			hold := true
			req.Hold = &hold
		}
	case "--begin":
		if req.BeginTime == nil {
			parsed, err := parseSlurmDirectiveTime(value)
			if err != nil {
				return fmt.Errorf("invalid #SBATCH begin time %q: %w", value, err)
			}
			req.BeginTime = &slurmDateTime{Time: parsed}
		}
	case "--deadline":
		if req.Deadline == nil {
			parsed, err := parseSlurmDirectiveTime(value)
			if err != nil {
				return fmt.Errorf("invalid #SBATCH deadline %q: %w", value, err)
			}
			req.Deadline = &slurmDateTime{Time: parsed}
		}
	case "-t", "--time":
		if req.TimeLimit == nil {
			seconds, err := parseSlurmTimeLimit(value)
			if err != nil {
				return fmt.Errorf("invalid #SBATCH time limit %q: %w", value, err)
			}
			limit := slurmTimeLimit(seconds)
			req.TimeLimit = &limit
		}
	case "-d", "--dependency":
		if req.Dependencies == "" {
			req.Dependencies = value
		}
	case "--reservation":
		if req.Reservation == "" {
			req.Reservation = value
		}
	case "-w", "--nodelist", "--node-list", "--nodeslist":
		if req.NodeList == "" {
			req.NodeList = value
		}
	case "-x", "--exclude":
		if req.ExcludeNodes == "" {
			req.ExcludeNodes = value
		}
	case "-C", "--constraint":
		if req.Constraint == "" {
			req.Constraint = value
		}
	case "--gres":
		if req.GRES == "" {
			req.GRES = value
		}
	case "--tres", "--tres-per-job":
		if req.TRES == "" {
			req.TRES = value
		}
	case "--licenses":
		if req.Licenses == "" {
			req.Licenses = value
		}
	case "-a", "--array":
		if req.Array == "" {
			req.Array = value
		}
	case "-J", "--job-name":
		if req.JobName == "" {
			req.JobName = value
		}
	case "-D", "--chdir":
		if req.WorkDir == "" {
			req.WorkDir = value
		}
	case "-i", "--input":
		if req.StdinPath == "" {
			req.StdinPath = value
		}
	case "-o", "--output":
		if req.StdoutPath == "" {
			req.StdoutPath = value
		}
	case "-e", "--error":
		if req.StderrPath == "" {
			req.StderrPath = value
		}
	case "--open-mode":
		if req.OpenMode == "" {
			req.OpenMode = value
		}
	case "--comment":
		if req.Comment == "" {
			req.Comment = value
		}
	case "--mail-type":
		if req.MailType == "" {
			req.MailType = value
		}
	case "--mail-user":
		if req.MailUser == "" {
			req.MailUser = value
		}
	case "--exclusive":
		if req.Exclusive == nil {
			exclusive := true
			req.Exclusive = &exclusive
		}
	case "--requeue":
		if req.Requeue == nil {
			requeue := true
			req.Requeue = &requeue
		}
	case "--no-requeue":
		if req.Requeue == nil {
			requeue := false
			req.Requeue = &requeue
		}
	case "--export":
		if req.Export == "" {
			req.Export = value
		}
	default:
		return nil
	}
	return nil
}

func parseSlurmDirectiveTime(value string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	var lastErr error
	for _, layout := range layouts {
		parsed, err := time.ParseInLocation(layout, value, time.Local)
		if err == nil {
			return parsed, nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}

func parseSlurmTimeLimit(value string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty time limit")
	}
	days := 0
	timeText := value
	hasDayComponent := false
	if before, after, ok := strings.Cut(value, "-"); ok {
		parsedDays, err := strconv.Atoi(before)
		if err != nil || parsedDays < 0 {
			return 0, fmt.Errorf("invalid day component")
		}
		days = parsedDays
		timeText = after
		hasDayComponent = true
	}
	parts := strings.Split(timeText, ":")
	total := days * 24 * 3600
	if hasDayComponent {
		switch len(parts) {
		case 1:
			hours, err := strconv.Atoi(parts[0])
			if err != nil || hours < 0 {
				return 0, fmt.Errorf("invalid hour component")
			}
			total += hours * 3600
		case 2:
			hours, err := strconv.Atoi(parts[0])
			if err != nil || hours < 0 {
				return 0, fmt.Errorf("invalid hour component")
			}
			minutes, err := strconv.Atoi(parts[1])
			if err != nil || minutes < 0 || minutes >= 60 {
				return 0, fmt.Errorf("invalid minute component")
			}
			total += hours*3600 + minutes*60
		case 3:
			hours, err := strconv.Atoi(parts[0])
			if err != nil || hours < 0 {
				return 0, fmt.Errorf("invalid hour component")
			}
			minutes, err := strconv.Atoi(parts[1])
			if err != nil || minutes < 0 || minutes >= 60 {
				return 0, fmt.Errorf("invalid minute component")
			}
			seconds, err := strconv.Atoi(parts[2])
			if err != nil || seconds < 0 || seconds >= 60 {
				return 0, fmt.Errorf("invalid second component")
			}
			total += hours*3600 + minutes*60 + seconds
		default:
			return 0, fmt.Errorf("invalid time limit format")
		}
	} else {
		switch len(parts) {
		case 1:
			minutes, err := strconv.Atoi(parts[0])
			if err != nil || minutes < 0 {
				return 0, fmt.Errorf("invalid minute component")
			}
			total += minutes * 60
		case 2:
			minutes, err := strconv.Atoi(parts[0])
			if err != nil || minutes < 0 {
				return 0, fmt.Errorf("invalid minute component")
			}
			seconds, err := strconv.Atoi(parts[1])
			if err != nil || seconds < 0 || seconds >= 60 {
				return 0, fmt.Errorf("invalid second component")
			}
			total += minutes*60 + seconds
		case 3:
			hours, err := strconv.Atoi(parts[0])
			if err != nil || hours < 0 {
				return 0, fmt.Errorf("invalid hour component")
			}
			minutes, err := strconv.Atoi(parts[1])
			if err != nil || minutes < 0 || minutes >= 60 {
				return 0, fmt.Errorf("invalid minute component")
			}
			seconds, err := strconv.Atoi(parts[2])
			if err != nil || seconds < 0 || seconds >= 60 {
				return 0, fmt.Errorf("invalid second component")
			}
			total += hours*3600 + minutes*60 + seconds
		default:
			return 0, fmt.Errorf("invalid time limit format")
		}
	}
	if total <= 0 {
		return 0, fmt.Errorf("time limit must be positive")
	}
	return total, nil
}

func parseSlurmNodeCount(value string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty node count")
	}
	if before, after, ok := strings.Cut(value, "-"); ok {
		minNodes, err := strconv.Atoi(strings.TrimSpace(before))
		if err != nil || minNodes <= 0 {
			return 0, fmt.Errorf("invalid minimum node count")
		}
		maxNodes, err := strconv.Atoi(strings.TrimSpace(after))
		if err != nil || maxNodes <= 0 || maxNodes < minNodes {
			return 0, fmt.Errorf("invalid maximum node count")
		}
		return minNodes, nil
	}
	nodes, err := strconv.Atoi(value)
	if err != nil || nodes <= 0 {
		return 0, fmt.Errorf("nodes must be positive")
	}
	return nodes, nil
}

func parseSlurmMemoryMB(value string) (int64, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return 0, fmt.Errorf("empty memory value")
	}

	numberPart := value
	unit := ""
	for i, r := range value {
		if r < '0' || r > '9' {
			numberPart = strings.TrimSpace(value[:i])
			unit = strings.TrimSpace(value[i:])
			break
		}
	}

	amount, err := strconv.ParseInt(numberPart, 10, 64)
	if err != nil || amount <= 0 {
		return 0, fmt.Errorf("memory must be positive")
	}
	unit = strings.TrimSuffix(strings.TrimSuffix(unit, "ib"), "b")
	switch unit {
	case "", "m":
		return amount, nil
	case "k":
		if amount <= 1024 {
			return 1, nil
		}
		return (amount + 1023) / 1024, nil
	case "g":
		return amount * 1024, nil
	case "t":
		return amount * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("unsupported memory unit %q", unit)
	}
}

func validateSlurmBatchResources(req slurmBatchRequest) error {
	if req.CPU != nil && *req.CPU <= 0 {
		return fmt.Errorf("cpus must be positive")
	}
	if req.NTasks != nil && *req.NTasks <= 0 {
		return fmt.Errorf("ntasks must be positive")
	}
	if req.CPUsPerTask != nil && *req.CPUsPerTask <= 0 {
		return fmt.Errorf("cpus_per_task must be positive")
	}
	if req.Nodes != nil {
		if *req.Nodes <= 0 {
			return fmt.Errorf("nodes must be positive")
		}
	}
	if req.Memory != nil && req.Memory.Int64() <= 0 {
		return fmt.Errorf("memory must be positive")
	}
	if req.MemoryPerCPU != nil && req.MemoryPerCPU.Int64() <= 0 {
		return fmt.Errorf("mem_per_cpu must be positive")
	}
	if err := validateSlurmLicenses(req.Licenses); err != nil {
		return err
	}
	return nil
}

func validateSlurmLicenses(licenses string) error {
	for _, raw := range slurmCSVValues(licenses) {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		name := item
		if before, after, ok := strings.Cut(item, ":"); ok {
			name = before
			count, err := strconv.Atoi(strings.TrimSpace(after))
			if err != nil || count <= 0 {
				return fmt.Errorf("invalid licenses count %q", item)
			}
		}
		if before, _, ok := strings.Cut(name, "@"); ok {
			name = before
		}
		name = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(strings.TrimPrefix(name, "licenses/"), "license/"), "license:"))
		if name == "" {
			return fmt.Errorf("invalid licenses value %q", item)
		}
	}
	return nil
}

func applySlurmBatchScheduling(sub *models.Submission, req slurmBatchRequest) {
	if jobName := strings.TrimSpace(req.JobName); jobName != "" {
		sub.JobName = jobName
	}
	if workDir := strings.TrimSpace(req.WorkDir); workDir != "" {
		sub.WorkDir = workDir
	}
	if stdinPath := strings.TrimSpace(req.StdinPath); stdinPath != "" {
		sub.StdinPath = stdinPath
	}
	if stdoutPath := strings.TrimSpace(req.StdoutPath); stdoutPath != "" {
		sub.StdoutPath = stdoutPath
	}
	if stderrPath := strings.TrimSpace(req.StderrPath); stderrPath != "" {
		sub.StderrPath = stderrPath
	}
	if openMode := strings.TrimSpace(req.OpenMode); openMode != "" {
		sub.OpenMode = openMode
	}
	if comment := strings.TrimSpace(req.Comment); comment != "" {
		sub.Comment = comment
	}
	if mailType := strings.TrimSpace(req.MailType); mailType != "" {
		sub.MailType = mailType
	}
	if mailUser := strings.TrimSpace(req.MailUser); mailUser != "" {
		sub.MailUser = mailUser
	}
	if req.Exclusive != nil {
		sub.Exclusive = *req.Exclusive
	}
	if req.Requeue != nil {
		sub.Requeue = *req.Requeue
	}
	if export := strings.TrimSpace(req.Export); export != "" {
		sub.ExportEnv = export
	}
	if len(req.Environment) > 0 {
		sub.Environment = req.Environment
	}
	if partition := strings.TrimSpace(req.Partition); partition != "" {
		sub.Cluster = partition
	}
	if req.Account != "" {
		sub.Account = req.Account
	}
	if req.QOS != "" {
		sub.QOS = req.QOS
	}
	if req.Priority != nil {
		sub.Priority = *req.Priority
	}
	if req.Nice != nil {
		sub.Nice = *req.Nice
	}
	if req.Hold != nil {
		sub.Hold = *req.Hold
		if sub.Hold && sub.Reason == "" {
			sub.Reason = "JobHeld"
		}
	}
	if req.CPU != nil {
		sub.CPU = *req.CPU
	}
	if req.NTasks != nil {
		sub.NTasks = *req.NTasks
	}
	if req.CPUsPerTask != nil {
		sub.CPUsPerTask = *req.CPUsPerTask
	}
	if req.Nodes != nil {
		sub.Nodes = *req.Nodes
	}
	if req.Memory != nil {
		sub.Memory = req.Memory.Int64()
	}
	if req.BeginTime != nil {
		sub.BeginTime = req.BeginTime.Ptr()
	}
	if req.Deadline != nil {
		sub.Deadline = req.Deadline.Ptr()
	}
	if req.TimeLimit != nil {
		sub.TimeLimit = req.TimeLimit.Int()
	}
	if req.Dependencies != "" {
		sub.Dependencies = req.Dependencies
	}
	if req.Reservation != "" {
		sub.Reservation = req.Reservation
	}
	if req.NodeList != "" {
		sub.NodeList = req.NodeList
	}
	if req.ExcludeNodes != "" {
		sub.ExcludeNodes = req.ExcludeNodes
	}
	if req.Constraint != "" {
		sub.Constraint = req.Constraint
	}
	if req.GRES != "" {
		sub.GRES = req.GRES
	}
	if req.TRES != "" {
		sub.TRES = req.TRES
	}
	if licenses := strings.TrimSpace(req.Licenses); licenses != "" {
		sub.Licenses = licenses
		sub.TRES = mergeSlurmLicensesIntoTRES(sub.TRES, licenses)
	}
}

func writeSlurmBatchFiles(basePath string, req slurmBatchRequest) error {
	if strings.TrimSpace(basePath) == "" {
		return fmt.Errorf("submission content storage is not configured")
	}
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return fmt.Errorf("failed to create submission directory: %w", err)
	}
	for name, content := range req.Files {
		if err := writeSlurmBatchFile(basePath, name, []byte(content)); err != nil {
			return err
		}
	}
	for name, encoded := range req.FilesBase64 {
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return fmt.Errorf("failed to decode base64 file %s: %w", name, err)
		}
		if err := writeSlurmBatchFile(basePath, name, data); err != nil {
			return err
		}
	}
	if req.Script != "" {
		scriptPath := req.ScriptPath
		if scriptPath == "" {
			scriptPath = "sbatch.sh"
		}
		if err := writeSlurmBatchFile(basePath, scriptPath, []byte(req.Script)); err != nil {
			return err
		}
	}
	return nil
}

func writeSlurmBatchFile(basePath, name string, data []byte) error {
	cleanName := filepath.Clean(name)
	if cleanName == "." || filepath.IsAbs(cleanName) || cleanName == ".." || strings.HasPrefix(cleanName, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("invalid file path: %s", name)
	}
	dst := filepath.Join(basePath, cleanName)
	rel, err := filepath.Rel(basePath, dst)
	if err != nil {
		return fmt.Errorf("invalid file path %s: %w", name, err)
	}
	if rel == "." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || rel == ".." {
		return fmt.Errorf("invalid file path: %s", name)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return fmt.Errorf("failed to create directory for %s: %w", name, err)
	}
	if err := os.WriteFile(dst, data, 0644); err != nil {
		return fmt.Errorf("failed to write %s: %w", name, err)
	}
	return nil
}
