package models

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
)

type Status string

const (
	StatusQueued    Status = "Queued"
	StatusRunning   Status = "Running"
	StatusSuspended Status = "Suspended"
	StatusSuccess   Status = "Success"
	StatusFailed    Status = "Failed"
)

type DevPodStatus string

const (
	DevPodStatusPending  DevPodStatus = "Pending"
	DevPodStatusCreating DevPodStatus = "Creating"
	DevPodStatusRunning  DevPodStatus = "Running"
	DevPodStatusStopped  DevPodStatus = "Stopped"
	DevPodStatusFailed   DevPodStatus = "Failed"
	DevPodStatusDeleting DevPodStatus = "Deleting"
	DevPodStatusDeleted  DevPodStatus = "Deleted"
	DevPodStatusExpired  DevPodStatus = "Expired"
)

// JSONMap is a helper type for storing JSON data in the database.
type JSONMap map[string]interface{}

func (m JSONMap) Value() (driver.Value, error) {
	return json.Marshal(m)
}

func (m *JSONMap) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, &m)
}

type User struct {
	ID        string `gorm:"primaryKey" json:"id"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	GitLabID     *string    `gorm:"uniqueIndex" json:"-"`
	Username     string     `gorm:"uniqueIndex" json:"username"`
	PasswordHash string     `json:"-"`
	Nickname     string     `json:"nickname"`
	Signature    string     `json:"signature"`
	AvatarURL    string     `json:"avatar_url"`
	BannedUntil  *time.Time `json:"banned_until"`
	BanReason    string     `json:"ban_reason"`
	DisableRank  bool       `gorm:"default:false" json:"disable_rank"`
	Tags         string     `gorm:"type:text" json:"tags"` // Comma-separated tags
}

type UserSSHKey struct {
	ID        string `gorm:"primaryKey" json:"id"`
	CreatedAt time.Time
	UpdatedAt time.Time

	UserID      string `gorm:"index" json:"user_id"`
	User        User   `gorm:"foreignKey:UserID" json:"user"`
	Name        string `json:"name"`
	PublicKey   string `gorm:"type:text" json:"public_key"`
	Fingerprint string `gorm:"uniqueIndex" json:"fingerprint"`
}

type DevPodSession struct {
	ID        string `gorm:"primaryKey" json:"id"`
	CreatedAt time.Time
	UpdatedAt time.Time

	UserID             string       `gorm:"index" json:"user_id"`
	User               User         `gorm:"foreignKey:UserID" json:"user"`
	Username           string       `gorm:"index" json:"username"`
	OwnerName          string       `gorm:"index" json:"owner_name"`
	Name               string       `gorm:"index" json:"name"`
	DisplayName        string       `json:"display_name"`
	Image              string       `json:"image"`
	CPU                int          `json:"cpu"`
	MemoryMB           int          `json:"memory_mb"`
	GPU                int          `json:"gpu"`
	StorageGB          int          `json:"storage_gb"`
	Persistent         bool         `json:"persistent"`
	NetworkMode        string       `json:"network_mode"`
	MPIEnabled         bool         `json:"mpi_enabled"`
	HostNetwork        bool         `json:"host_network"`
	Status             DevPodStatus `gorm:"index" json:"status"`
	Namespace          string       `json:"namespace"`
	K8sResourceName    string       `gorm:"index" json:"k8s_resource_name"`
	SSHUser            string       `json:"ssh_user"`
	SSHHost            string       `json:"ssh_host"`
	SSHPort            int          `json:"ssh_port"`
	SSHCommand         string       `json:"ssh_command"`
	IdleTimeoutSeconds int          `json:"idle_timeout_seconds"`
	MaxLifetimeSeconds int          `json:"max_lifetime_seconds"`
	Env                JSONMap      `gorm:"type:text" json:"env"`
	Command            string       `gorm:"type:text" json:"command"`
	ExpiresAt          time.Time    `gorm:"index" json:"expires_at"`
	LastActivityAt     *time.Time   `json:"last_activity_at"`
	LastError          string       `gorm:"type:text" json:"last_error"`
}

type DevPodAuditRecord struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	CreatedAt time.Time `gorm:"index" json:"created_at"`

	UserID         string `gorm:"index" json:"user_id"`
	Username       string `gorm:"index" json:"username"`
	Action         string `gorm:"index" json:"action"`
	DevPodID       string `gorm:"index" json:"devpod_id"`
	ResourceName   string `gorm:"index" json:"resource_name"`
	Image          string `json:"image"`
	CPU            int    `json:"cpu"`
	MemoryMB       int    `json:"memory_mb"`
	GPU            int    `json:"gpu"`
	NetworkProfile string `json:"network_profile"`
	HostNetwork    bool   `json:"host_network"`
	MPIEnabled     bool   `json:"mpi_enabled"`
	SourceIP       string `json:"source_ip"`
	Result         string `gorm:"index" json:"result"`
	ErrorMessage   string `gorm:"type:text" json:"error_message"`
}

type Submission struct {
	ID        string `gorm:"primaryKey" json:"id"`
	CreatedAt time.Time
	UpdatedAt time.Time

	ProblemID string `gorm:"index" json:"problem_id"`
	UserID    string `gorm:"index" json:"user_id"`
	User      User   `json:"user"`

	Status             Status     `gorm:"index" json:"status"`
	CurrentStep        int        `json:"current_step"` // index of the current workflow step
	JobName            string     `gorm:"index" json:"job_name"`
	WorkDir            string     `json:"work_dir"`
	StdinPath          string     `json:"stdin_path"`
	StdoutPath         string     `json:"stdout_path"`
	StderrPath         string     `json:"stderr_path"`
	OpenMode           string     `json:"open_mode"`
	Comment            string     `json:"comment"`
	MailType           string     `json:"mail_type"`
	MailUser           string     `json:"mail_user"`
	Exclusive          bool       `json:"exclusive"`
	Requeue            bool       `json:"requeue"`
	ExportEnv          string     `json:"export"`
	Environment        JSONMap    `gorm:"type:text" json:"environment"`
	Cluster            string     `json:"cluster"`
	Node               string     `json:"node"`
	AllocatedCores     string     `json:"allocated_cores"`      // e.g., "2,3,4"
	AllocatedNodeCores string     `json:"allocated_node_cores"` // e.g., "node-a:0,1;node-b:0,1"
	CPU                int        `json:"cpu"`
	NTasks             int        `json:"ntasks"`
	CPUsPerTask        int        `json:"cpus_per_task"`
	Nodes              int        `json:"nodes"`
	Memory             int64      `json:"memory"`
	Account            string     `gorm:"index" json:"account"`
	QOS                string     `gorm:"index" json:"qos"`
	Priority           int        `gorm:"index" json:"priority"`
	Nice               int        `json:"nice"`
	Hold               bool       `gorm:"index" json:"hold"`
	BeginTime          *time.Time `gorm:"index" json:"begin_time"`
	Deadline           *time.Time `gorm:"index" json:"deadline"`
	TimeLimit          int        `json:"time_limit"` // seconds
	Dependencies       string     `json:"dependencies"`
	Reservation        string     `json:"reservation"`
	NodeList           string     `json:"nodelist"`
	ExcludeNodes       string     `json:"exclude_nodes"`
	Constraint         string     `json:"constraint"`
	GRES               string     `json:"gres"`
	TRES               string     `json:"tres"`
	Licenses           string     `json:"licenses"`
	BillingUnits       float64    `json:"billing_units"`
	Reason             string     `json:"reason"`
	SlurmState         string     `gorm:"-" json:"slurm_state,omitempty"`
	SlurmReason        string     `gorm:"-" json:"slurm_reason,omitempty"`
	ArrayJobID         string     `gorm:"index" json:"array_job_id"`
	ArrayTaskID        int        `gorm:"index" json:"array_task_id"`
	ArraySpec          string     `json:"array_spec"`
	ArrayTaskCount     int        `json:"array_task_count"`
	ArrayMaxRunning    int        `json:"array_max_running"`
	Score              int        `json:"score"`
	Performance        float64    `json:"performance"`
	Info               JSONMap    `gorm:"type:text" json:"info"`
	IsValid            bool       `json:"is_valid"`

	Containers []Container `gorm:"foreignKey:SubmissionID;constraint:OnDelete:CASCADE" json:"containers"`
}

type Container struct {
	ID        string `gorm:"primaryKey" json:"id"`
	CreatedAt time.Time
	UpdatedAt time.Time

	SubmissionID string `gorm:"index" json:"submission_id"`
	UserID       string `gorm:"index" json:"user_id"`
	User         User   `gorm:"foreignKey:UserID" json:"user"`
	DockerID     string `gorm:"docker_id" json:"docker_id"`

	Image       string    `json:"image"`
	Status      Status    `json:"status"`
	ExitCode    int       `json:"exit_code"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	LogFilePath string    `json:"log_file_path"`
}

type AllocationStatus string

const (
	AllocationActive   AllocationStatus = "Active"
	AllocationReleased AllocationStatus = "Released"
)

type Allocation struct {
	ID        string `gorm:"primaryKey" json:"id"`
	CreatedAt time.Time
	UpdatedAt time.Time

	ReleasedAt *time.Time       `json:"released_at"`
	Status     AllocationStatus `gorm:"index" json:"status"`
	UserID     string           `gorm:"index" json:"user_id"`
	Cluster    string           `gorm:"index" json:"cluster"`
	Node       string           `json:"node"`

	CPU                int     `json:"cpu"`
	Memory             int64   `json:"memory"`
	Nodes              int     `json:"nodes"`
	AllocatedCores     string  `json:"allocated_cores"`
	AllocatedNodeCores string  `json:"allocated_node_cores"`
	Account            string  `gorm:"index" json:"account"`
	QOS                string  `gorm:"index" json:"qos"`
	TRES               string  `json:"tres"`
	GRES               string  `json:"gres"`
	BillingUnits       float64 `json:"billing_units"`
	TimeLimit          int     `json:"time_limit"`
	Constraint         string  `json:"constraint"`
	Reservation        string  `json:"reservation"`
	NodeList           string  `json:"nodelist"`
	ExcludeNodes       string  `json:"exclude_nodes"`
	Exclusive          bool    `json:"exclusive"`
	Reason             string  `json:"reason"`
}

type RunStep struct {
	ID        string `gorm:"primaryKey" json:"id"`
	CreatedAt time.Time
	UpdatedAt time.Time

	AllocationID string `gorm:"index" json:"allocation_id"`
	UserID       string `gorm:"index" json:"user_id"`
	Cluster      string `gorm:"index" json:"cluster"`
	Node         string `json:"node"`

	ContainerID string `gorm:"index" json:"container_id"`
	Image       string `json:"image"`
	Runtime     string `json:"runtime"`
	Command     string `gorm:"type:text" json:"command"`
	Status      Status `gorm:"index" json:"status"`
	ExitCode    int    `json:"exit_code"`
	Stdout      string `gorm:"type:text" json:"stdout"`
	Stderr      string `gorm:"type:text" json:"stderr"`
	Reason      string `json:"reason"`
	Timeout     int    `json:"timeout"`

	CPU            int       `json:"cpu"`
	Memory         int64     `json:"memory"`
	AllocatedCores string    `json:"allocated_cores"`
	AveCPU         float64   `json:"ave_cpu"`
	AveRSS         int64     `json:"ave_rss"`
	MaxRSS         int64     `json:"max_rss"`
	MaxVMSize      int64     `json:"max_vmsize"`
	StartedAt      time.Time `json:"started_at"`
	FinishedAt     time.Time `json:"finished_at"`
}

type AccountingRecord struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	CreatedAt time.Time `gorm:"index" json:"created_at"`

	SubmissionID string `gorm:"index" json:"submission_id"`
	ContainerID  string `gorm:"index" json:"container_id"`
	UserID       string `gorm:"index" json:"user_id"`
	ProblemID    string `gorm:"index" json:"problem_id"`
	JobName      string `gorm:"index" json:"job_name"`
	Cluster      string `gorm:"index" json:"cluster"`
	Node         string `gorm:"index" json:"node"`
	Account      string `gorm:"index" json:"account"`
	QOS          string `gorm:"index" json:"qos"`
	ArrayJobID   string `gorm:"index" json:"array_job_id"`
	ArrayTaskID  int    `gorm:"index" json:"array_task_id"`

	Event        string  `gorm:"index" json:"event"`
	State        Status  `gorm:"index" json:"state"`
	StepName     string  `json:"step_name"`
	ExitCode     int     `json:"exit_code"`
	CPU          int     `json:"cpu"`
	Memory       int64   `json:"memory"`
	TRES         string  `json:"tres"`
	BillingUnits float64 `json:"billing_units"`
	Reason       string  `json:"reason"`
	Message      string  `json:"message"`
	Score        int     `json:"score"`
	Performance  float64 `json:"performance"`
}

type SlurmTrigger struct {
	ID        string `gorm:"primaryKey" json:"id"`
	CreatedAt time.Time
	UpdatedAt time.Time

	Name       string     `gorm:"index" json:"name"`
	Event      string     `gorm:"index" json:"event"`
	JobID      string     `gorm:"index" json:"job_id"`
	UserID     string     `gorm:"index" json:"user_id"`
	Partition  string     `gorm:"index" json:"partition"`
	Node       string     `gorm:"index" json:"node"`
	State      string     `json:"state"`
	Action     string     `json:"action"`
	Program    string     `json:"program"`
	Flags      string     `json:"flags"`
	Active     bool       `gorm:"index" json:"active"`
	FiredAt    *time.Time `json:"fired_at"`
	MatchCount int        `json:"match_count"`
	Message    string     `json:"message"`
}

type SlurmCronJob struct {
	ID        string `gorm:"primaryKey" json:"id"`
	CreatedAt time.Time
	UpdatedAt time.Time

	Name      string     `gorm:"index" json:"name"`
	Schedule  string     `json:"schedule"`
	BatchJSON string     `gorm:"type:text" json:"batch_json"`
	Enabled   bool       `gorm:"index" json:"enabled"`
	UserID    string     `gorm:"index" json:"user_id"`
	ProblemID string     `gorm:"index" json:"problem_id"`
	NextRunAt *time.Time `gorm:"index" json:"next_run_at"`
	LastRunAt *time.Time `json:"last_run_at"`
	LastJobID string     `json:"last_job_id"`
	RunCount  int        `json:"run_count"`
	Message   string     `json:"message"`
}

type ContestScoreHistory struct {
	ID                        uint `gorm:"primaryKey"`
	CreatedAt                 time.Time
	UserID                    string
	ContestID                 string
	ProblemID                 string
	TotalScoreAfterChange     int
	LastEffectiveSubmissionID string
}

type UserProblemBestScore struct {
	ID              uint   `gorm:"primaryKey"`
	UserID          string `gorm:"uniqueIndex:idx_user_problem"`
	ContestID       string `gorm:"uniqueIndex:idx_user_problem"`
	ProblemID       string `gorm:"uniqueIndex:idx_user_problem"`
	Score           int
	Performance     float64
	SubmissionID    string
	SubmissionCount int
	LastScoreTime   time.Time
}
