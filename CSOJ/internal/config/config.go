package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type CORS struct {
	AllowedOrigins []string `yaml:"allowed_origins"`
}

type Link struct {
	Name string `yaml:"name" json:"name"`
	URL  string `yaml:"url"  json:"url"`
}

type Config struct {
	Cluster      []Cluster `yaml:"cluster"`
	Scheduler    Scheduler `yaml:"scheduler"`
	ContestsRoot string    `yaml:"contests_root"`
	Logger       Logger    `yaml:"logger"`
	Storage      Storage   `yaml:"storage"`
	Auth         Auth      `yaml:"auth"`
	Listen       string    `yaml:"listen"`
	Admin        Admin     `yaml:"admin"`
	CORS         CORS      `yaml:"cors"`
	Mail         Mail      `yaml:"mail"`
	Links        []Link    `yaml:"links"`
	DevPod       DevPod    `yaml:"devpod"`
}

type Cluster struct {
	Name          string   `yaml:"name" json:"name"`
	State         string   `yaml:"state" json:"state"`
	PriorityTier  int      `yaml:"priority_tier" json:"priority_tier"`
	MaxTime       int      `yaml:"max_time" json:"max_time"`
	MaxJobs       int      `yaml:"max_jobs" json:"max_jobs"`
	AllowUsers    []string `yaml:"allow_users" json:"allow_users"`
	AllowAccounts []string `yaml:"allow_accounts" json:"allow_accounts"`
	AllowQOS      []string `yaml:"allow_qos" json:"allow_qos"`
	DenyQOS       []string `yaml:"deny_qos" json:"deny_qos"`
	Nodes         []Node   `yaml:"node" json:"node"`
}

type DockerConfig struct {
	Host      string `yaml:"host"`
	TLSVerify bool   `yaml:"tls_verify"`
	CACert    string `yaml:"ca_cert"`
	Cert      string `yaml:"cert"`
	Key       string `yaml:"key"`
}

type KubernetesToleration struct {
	Key               string `yaml:"key" json:"key"`
	Operator          string `yaml:"operator" json:"operator"`
	Value             string `yaml:"value" json:"value"`
	Effect            string `yaml:"effect" json:"effect"`
	TolerationSeconds *int64 `yaml:"toleration_seconds" json:"toleration_seconds"`
}

type KubernetesConfig struct {
	Kubectl               string                 `yaml:"kubectl" json:"kubectl"`
	Kubeconfig            string                 `yaml:"kubeconfig" json:"kubeconfig"`
	Context               string                 `yaml:"context" json:"context"`
	Namespace             string                 `yaml:"namespace" json:"namespace"`
	ServiceAccount        string                 `yaml:"service_account" json:"service_account"`
	ImagePullSecrets      []string               `yaml:"image_pull_secrets" json:"image_pull_secrets"`
	NodeSelector          map[string]string      `yaml:"node_selector" json:"node_selector"`
	Tolerations           []KubernetesToleration `yaml:"tolerations" json:"tolerations"`
	PriorityClassName     string                 `yaml:"priority_class_name" json:"priority_class_name"`
	RuntimeClassName      string                 `yaml:"runtime_class_name" json:"runtime_class_name"`
	StorageClassName      string                 `yaml:"storage_class_name" json:"storage_class_name"`
	WorkdirSize           string                 `yaml:"workdir_size" json:"workdir_size"`
	StartupTimeoutSeconds int                    `yaml:"startup_timeout_seconds" json:"startup_timeout_seconds"`
	RunnerContainerName   string                 `yaml:"runner_container_name" json:"runner_container_name"`
	RunnerCommand         []string               `yaml:"runner_command" json:"runner_command"`
}

type Node struct {
	Name       string           `yaml:"name" json:"name"`
	CPU        int              `yaml:"cpu" json:"cpu"`
	Memory     int64            `yaml:"memory" json:"memory"`
	State      string           `yaml:"state" json:"state"`
	Reason     string           `yaml:"reason" json:"reason"`
	Features   []string         `yaml:"features" json:"features"`
	GRES       []string         `yaml:"gres" json:"gres"`
	Weight     int              `yaml:"weight" json:"weight"`
	Runtime    string           `yaml:"runtime" json:"runtime"`
	Docker     DockerConfig     `yaml:"docker" json:"docker"`
	Kubernetes KubernetesConfig `yaml:"kubernetes" json:"kubernetes"`
}

type Scheduler struct {
	QueueSize       int                `yaml:"queue_size" json:"queue_size"`
	Backfill        *bool              `yaml:"backfill" json:"backfill"`
	PriorityWeights PriorityWeights    `yaml:"priority_weights" json:"priority_weights"`
	BillingWeights  map[string]float64 `yaml:"billing_weights" json:"billing_weights"`
	Licenses        map[string]int     `yaml:"licenses" json:"licenses"`
	FairshareDecay  FairshareDecay     `yaml:"fairshare_decay" json:"fairshare_decay"`
	QOS             []QOS              `yaml:"qos" json:"qos"`
	Accounts        []Account          `yaml:"accounts" json:"accounts"`
	Reservations    []Reservation      `yaml:"reservations" json:"reservations"`
}

type PriorityWeights struct {
	Age       int `yaml:"age" json:"age"`
	QOS       int `yaml:"qos" json:"qos"`
	Nice      int `yaml:"nice" json:"nice"`
	Partition int `yaml:"partition" json:"partition"`
	JobSize   int `yaml:"job_size" json:"job_size"`
	Fairshare int `yaml:"fairshare" json:"fairshare"`
}

type FairshareDecay struct {
	Enabled       bool    `yaml:"enabled" json:"enabled"`
	HalfLifeHours float64 `yaml:"half_life_hours" json:"half_life_hours"`
	UsageWeight   float64 `yaml:"usage_weight" json:"usage_weight"`
}

type QOS struct {
	Name                     string   `yaml:"name" json:"name"`
	Priority                 int      `yaml:"priority" json:"priority"`
	MaxJobsPerUser           int      `yaml:"max_jobs_per_user" json:"max_jobs_per_user"`
	MaxSubmitJobsPerUser     int      `yaml:"max_submit_jobs_per_user" json:"max_submit_jobs_per_user"`
	MaxCPUPerJob             int      `yaml:"max_cpu_per_job" json:"max_cpu_per_job"`
	MaxMemoryPerJob          int64    `yaml:"max_memory_per_job" json:"max_memory_per_job"`
	MaxBillingPerJob         float64  `yaml:"max_billing_per_job" json:"max_billing_per_job"`
	MaxBillingPerUserRunning float64  `yaml:"max_billing_per_user_running" json:"max_billing_per_user_running"`
	MaxBillingPerUserSubmit  float64  `yaml:"max_billing_per_user_submit" json:"max_billing_per_user_submit"`
	MaxTime                  int      `yaml:"max_time" json:"max_time"`
	Preempt                  []string `yaml:"preempt" json:"preempt"`
}

type Account struct {
	Name              string   `yaml:"name" json:"name"`
	Users             []string `yaml:"users" json:"users"`
	AllowQOS          []string `yaml:"allow_qos" json:"allow_qos"`
	MaxJobs           int      `yaml:"max_jobs" json:"max_jobs"`
	MaxSubmit         int      `yaml:"max_submit" json:"max_submit"`
	MaxBillingRunning float64  `yaml:"max_billing_running" json:"max_billing_running"`
	MaxBillingSubmit  float64  `yaml:"max_billing_submit" json:"max_billing_submit"`
	Fairshare         int      `yaml:"fairshare" json:"fairshare"`
	ParentName        string   `yaml:"parent" json:"parent"`
}

type Reservation struct {
	Name          string    `yaml:"name" json:"name"`
	Cluster       string    `yaml:"cluster" json:"cluster"`
	Nodes         []string  `yaml:"nodes" json:"nodes"`
	Users         []string  `yaml:"users" json:"users"`
	Accounts      []string  `yaml:"accounts" json:"accounts"`
	StartTime     time.Time `yaml:"starttime" json:"starttime"`
	EndTime       time.Time `yaml:"endtime" json:"endtime"`
	CPU           int       `yaml:"cpu" json:"cpu"`
	Memory        int64     `yaml:"memory" json:"memory"`
	AllowOverlap  bool      `yaml:"allow_overlap" json:"allow_overlap"`
	IgnoreRunning bool      `yaml:"ignore_running" json:"ignore_running"`
}

type Logger struct {
	Level string `yaml:"level"`
	File  string `yaml:"file"`
}

type Storage struct {
	UserAvatar        string `yaml:"user_avatar"`
	SubmissionContent string `yaml:"submission_content"`
	Database          string `yaml:"database"`
	SubmissionLog     string `yaml:"submission_log"`
}

type Auth struct {
	JWT    JWT    `yaml:"jwt"`
	GitLab GitLab `yaml:"gitlab"`
	Local  Local  `yaml:"local"`
}

// Local defines configuration for username/password authentication.
type Local struct {
	Enabled bool `yaml:"enabled"`
}

type JWT struct {
	Secret      string `yaml:"secret"`
	ExpireHours int    `yaml:"expire_hours"`
}

type GitLab struct {
	App                 string `yaml:"app"`
	URL                 string `yaml:"url"`
	ClientID            string `yaml:"client_id"`
	ClientSecret        string `yaml:"client_secret"`
	RedirectURI         string `yaml:"redirect_uri"`
	FrontendCallbackURL string `yaml:"frontend_callback_url"`
}

type Admin struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"`
}

type Mail struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"-"`
	From     string `yaml:"from" json:"from"`
}

type DevPod struct {
	Enabled                 bool                   `yaml:"enabled" json:"enabled"`
	Mode                    string                 `yaml:"mode" json:"mode"`
	Kubectl                 string                 `yaml:"kubectl" json:"kubectl"`
	Kubeconfig              string                 `yaml:"kubeconfig" json:"-"`
	Context                 string                 `yaml:"context" json:"context"`
	Namespace               string                 `yaml:"namespace" json:"namespace"`
	ServiceAccount          string                 `yaml:"service_account" json:"service_account"`
	StorageClassName        string                 `yaml:"storage_class_name" json:"storage_class_name"`
	NodeSelector            map[string]string      `yaml:"node_selector" json:"node_selector"`
	HostNetworkNodeSelector map[string]string      `yaml:"host_network_node_selector" json:"host_network_node_selector"`
	Tolerations             []KubernetesToleration `yaml:"tolerations" json:"tolerations"`
	ImagePullSecrets        []string               `yaml:"image_pull_secrets" json:"image_pull_secrets"`
	PriorityClassName       string                 `yaml:"priority_class_name" json:"priority_class_name"`
	RuntimeClassName        string                 `yaml:"runtime_class_name" json:"runtime_class_name"`
	PrivilegedUserTags      []string               `yaml:"privileged_user_tags" json:"privileged_user_tags"`
	Gateway                 DevPodGateway          `yaml:"gateway" json:"gateway"`
	Defaults                DevPodDefaults         `yaml:"defaults" json:"defaults"`
	Limits                  DevPodLimits           `yaml:"limits" json:"limits"`
	Images                  []DevPodImage          `yaml:"images" json:"images"`
	NetworkProfiles         []DevPodNetworkProfile `yaml:"network_profiles" json:"network_profiles"`
}

type DevPodGateway struct {
	Host        string `yaml:"host" json:"host"`
	Port        int    `yaml:"port" json:"port"`
	BackendPort int    `yaml:"backend_port" json:"backend_port"`
	Namespace   string `yaml:"namespace" json:"namespace"`
}

type DevPodDefaults struct {
	Image              string   `yaml:"image" json:"image"`
	CPU                int      `yaml:"cpu" json:"cpu"`
	MemoryMB           int      `yaml:"memory_mb" json:"memory_mb"`
	GPU                int      `yaml:"gpu" json:"gpu"`
	StorageGB          int      `yaml:"storage_gb" json:"storage_gb"`
	Persistent         bool     `yaml:"persistent" json:"persistent"`
	NetworkProfile     string   `yaml:"network_profile" json:"network_profile"`
	IdleTimeoutSeconds int      `yaml:"idle_timeout_seconds" json:"idle_timeout_seconds"`
	MaxLifetimeSeconds int      `yaml:"max_lifetime_seconds" json:"max_lifetime_seconds"`
	Shell              string   `yaml:"shell" json:"shell"`
	MountPath          string   `yaml:"mount_path" json:"mount_path"`
	Command            []string `yaml:"command" json:"command"`
}

type DevPodLimits struct {
	MaxPodsPerUser     int `yaml:"max_pods_per_user" json:"max_pods_per_user"`
	MaxCPUPerPod       int `yaml:"max_cpu_per_pod" json:"max_cpu_per_pod"`
	MaxMemoryMBPerPod  int `yaml:"max_memory_mb_per_pod" json:"max_memory_mb_per_pod"`
	MaxGPUPerPod       int `yaml:"max_gpu_per_pod" json:"max_gpu_per_pod"`
	MaxStorageGBPerPod int `yaml:"max_storage_gb_per_pod" json:"max_storage_gb_per_pod"`
	MaxEnvVarsPerPod   int `yaml:"max_env_vars_per_pod" json:"max_env_vars_per_pod"`
	MaxCommandArgs     int `yaml:"max_command_args" json:"max_command_args"`
}

type DevPodImage struct {
	Name        string   `yaml:"name" json:"name"`
	Image       string   `yaml:"image" json:"image"`
	Allowed     bool     `yaml:"allowed" json:"allowed"`
	Profiles    []string `yaml:"profiles" json:"profiles"`
	AllowedTags []string `yaml:"allowed_tags" json:"allowed_tags"`
}

type DevPodNetworkProfile struct {
	Name          string                 `yaml:"name" json:"name"`
	AllowInternet bool                   `yaml:"allow_internet" json:"allow_internet"`
	HostNetwork   bool                   `yaml:"host_network" json:"host_network"`
	AdminOnly     bool                   `yaml:"admin_only" json:"admin_only"`
	MPI           bool                   `yaml:"mpi" json:"mpi"`
	NodeSelector  map[string]string      `yaml:"node_selector" json:"node_selector"`
	Tolerations   []KubernetesToleration `yaml:"tolerations" json:"tolerations"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
