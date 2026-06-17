package devpod

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

const (
	ModeCRD = "crd"

	defaultKubectl            = "kubectl"
	defaultNamespace          = "devpods"
	defaultGatewayPort        = 22
	defaultGatewayBackendPort = 2222
	defaultGatewayNamespace   = "devpod-system"
	defaultImage              = "ubuntu:24.04"
	defaultCPU                = 1
	defaultMemoryMB           = 2048
	defaultStorageGB          = 10
	defaultIdleTimeout        = 3600
	defaultMaxLifetime        = 86400
	defaultNetworkProfile     = "default"
	defaultShell              = "bash"
	defaultMountPath          = "/home/devpod"
	defaultMaxPodsPerUser     = 3
	defaultMaxCPU             = 8
	defaultMaxMemoryMB        = 32768
	defaultMaxGPU             = 1
	defaultMaxStorageGB       = 100
	defaultMaxEnvVars         = 64
	defaultMaxCommandArgs     = 32
)

var (
	envNamePattern      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	devpodUserPattern   = regexp.MustCompile(`[^a-z0-9-]+`)
	sshKeyTypeAllowlist = map[string]bool{"ssh-ed25519": true, "ssh-rsa": true, "ecdsa-sha2-nistp256": true, "ecdsa-sha2-nistp384": true, "ecdsa-sha2-nistp521": true}
)

type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return e.Message
}

func newAPIError(status int, code, message string) *APIError {
	return &APIError{Status: status, Code: code, Message: message}
}

type Manager struct {
	cfg config.DevPod
	run func(ctx context.Context, input []byte, namespaced bool, args ...string) ([]byte, error)
}

func NewManager(cfg config.DevPod) *Manager {
	cfg = NormalizeConfig(cfg)
	m := &Manager{cfg: cfg}
	m.run = m.runKubectl
	return m
}

func (m *Manager) Config() config.DevPod {
	return m.cfg
}

type CreateRequest struct {
	DisplayName        string            `json:"displayName"`
	Image              string            `json:"image"`
	CPU                int               `json:"cpu"`
	MemoryMB           int               `json:"memoryMB"`
	GPU                int               `json:"gpu"`
	Persistent         bool              `json:"persistent"`
	StorageGB          int               `json:"storageGB"`
	IdleTimeoutSeconds int               `json:"idleTimeoutSeconds"`
	MaxLifetimeSeconds int               `json:"maxLifetimeSeconds"`
	NetworkProfile     string            `json:"networkProfile"`
	MPIEnabled         bool              `json:"mpiEnabled"`
	HostNetwork        bool              `json:"hostNetwork"`
	Env                map[string]string `json:"env"`
	Command            []string          `json:"command"`
}

type CreatePlan struct {
	Request        CreateRequest
	OwnerName      string
	Image          config.DevPodImage
	NetworkProfile config.DevPodNetworkProfile
	Command        []string
	Env            map[string]string
	ExpiresAt      time.Time
}

type Options struct {
	Enabled         bool                          `json:"enabled"`
	Mode            string                        `json:"mode"`
	Gateway         config.DevPodGateway          `json:"gateway"`
	Defaults        config.DevPodDefaults         `json:"defaults"`
	Limits          config.DevPodLimits           `json:"limits"`
	Images          []config.DevPodImage          `json:"images"`
	NetworkProfiles []config.DevPodNetworkProfile `json:"network_profiles"`
	SSHKeyRequired  bool                          `json:"ssh_key_required"`
}

type RemoteStatus struct {
	Phase            string
	Endpoint         string
	WorkloadName     string
	LastActivityTime *time.Time
}

func NormalizeConfig(in config.DevPod) config.DevPod {
	if in.Mode == "" {
		in.Mode = ModeCRD
	}
	if in.Kubectl == "" {
		in.Kubectl = defaultKubectl
	}
	if in.Namespace == "" {
		in.Namespace = defaultNamespace
	}
	if in.Gateway.Port <= 0 {
		in.Gateway.Port = defaultGatewayPort
	}
	if in.Gateway.BackendPort <= 0 {
		in.Gateway.BackendPort = defaultGatewayBackendPort
	}
	if in.Gateway.Namespace == "" {
		in.Gateway.Namespace = defaultGatewayNamespace
	}
	if in.Defaults.Image == "" {
		in.Defaults.Image = defaultImage
	}
	if in.Defaults.CPU <= 0 {
		in.Defaults.CPU = defaultCPU
	}
	if in.Defaults.MemoryMB <= 0 {
		in.Defaults.MemoryMB = defaultMemoryMB
	}
	if in.Defaults.StorageGB <= 0 {
		in.Defaults.StorageGB = defaultStorageGB
	}
	if in.Defaults.NetworkProfile == "" {
		in.Defaults.NetworkProfile = defaultNetworkProfile
	}
	if in.Defaults.IdleTimeoutSeconds <= 0 {
		in.Defaults.IdleTimeoutSeconds = defaultIdleTimeout
	}
	if in.Defaults.MaxLifetimeSeconds <= 0 {
		in.Defaults.MaxLifetimeSeconds = defaultMaxLifetime
	}
	if in.Defaults.Shell == "" {
		in.Defaults.Shell = defaultShell
	}
	if in.Defaults.MountPath == "" {
		in.Defaults.MountPath = defaultMountPath
	}
	if len(in.Defaults.Command) == 0 {
		in.Defaults.Command = []string{"sleep", "infinity"}
	}
	if in.Limits.MaxPodsPerUser <= 0 {
		in.Limits.MaxPodsPerUser = defaultMaxPodsPerUser
	}
	if in.Limits.MaxCPUPerPod <= 0 {
		in.Limits.MaxCPUPerPod = defaultMaxCPU
	}
	if in.Limits.MaxMemoryMBPerPod <= 0 {
		in.Limits.MaxMemoryMBPerPod = defaultMaxMemoryMB
	}
	if in.Limits.MaxGPUPerPod <= 0 {
		in.Limits.MaxGPUPerPod = defaultMaxGPU
	}
	if in.Limits.MaxStorageGBPerPod <= 0 {
		in.Limits.MaxStorageGBPerPod = defaultMaxStorageGB
	}
	if in.Limits.MaxEnvVarsPerPod <= 0 {
		in.Limits.MaxEnvVarsPerPod = defaultMaxEnvVars
	}
	if in.Limits.MaxCommandArgs <= 0 {
		in.Limits.MaxCommandArgs = defaultMaxCommandArgs
	}
	if len(in.PrivilegedUserTags) == 0 {
		in.PrivilegedUserTags = []string{"admin", "devpod-admin", "mpi"}
	}
	if len(in.Images) == 0 {
		in.Images = []config.DevPodImage{{
			Name:    "Ubuntu 24.04",
			Image:   defaultImage,
			Allowed: true,
		}}
	}
	if len(in.NetworkProfiles) == 0 {
		in.NetworkProfiles = []config.DevPodNetworkProfile{
			{Name: defaultNetworkProfile},
			{Name: "internet", AllowInternet: true},
			{Name: "mpi-hostnet", HostNetwork: true, AdminOnly: true, MPI: true},
		}
	}
	return in
}

func (m *Manager) Options() Options {
	return Options{
		Enabled:         m.cfg.Enabled,
		Mode:            m.cfg.Mode,
		Gateway:         m.cfg.Gateway,
		Defaults:        m.cfg.Defaults,
		Limits:          m.cfg.Limits,
		Images:          allowedImages(m.cfg.Images),
		NetworkProfiles: m.cfg.NetworkProfiles,
		SSHKeyRequired:  true,
	}
}

func (m *Manager) ValidateCreateRequest(user models.User, sshKeys []models.UserSSHKey, openPods int64, req CreateRequest) (CreatePlan, error) {
	if !m.cfg.Enabled {
		return CreatePlan{}, newAPIError(http.StatusNotFound, "DEVPOD_DISABLED", "DevPod is not enabled")
	}
	if strings.ToLower(strings.TrimSpace(m.cfg.Mode)) != ModeCRD {
		return CreatePlan{}, newAPIError(http.StatusBadRequest, "DEVPOD_MODE_UNSUPPORTED", "only devpod CRD mode is supported")
	}
	if strings.TrimSpace(m.cfg.Gateway.Host) == "" {
		return CreatePlan{}, newAPIError(http.StatusInternalServerError, "GATEWAY_NOT_CONFIGURED", "DevPod gateway host is not configured")
	}
	if len(sshKeys) == 0 {
		return CreatePlan{}, newAPIError(http.StatusBadRequest, "SSH_KEY_REQUIRED", "upload at least one SSH public key before creating a DevPod")
	}
	if m.cfg.Limits.MaxPodsPerUser > 0 && openPods >= int64(m.cfg.Limits.MaxPodsPerUser) {
		return CreatePlan{}, newAPIError(http.StatusForbidden, "QUOTA_EXCEEDED", "maximum DevPod count per user exceeded")
	}

	req.DisplayName = strings.TrimSpace(req.DisplayName)
	if req.DisplayName == "" {
		req.DisplayName = "DevPod"
	}
	if len(req.DisplayName) > 64 {
		return CreatePlan{}, newAPIError(http.StatusBadRequest, "INVALID_DISPLAY_NAME", "displayName must be at most 64 characters")
	}
	if req.Image == "" {
		req.Image = m.cfg.Defaults.Image
	}
	if req.CPU <= 0 {
		req.CPU = m.cfg.Defaults.CPU
	}
	if req.MemoryMB <= 0 {
		req.MemoryMB = m.cfg.Defaults.MemoryMB
	}
	if req.StorageGB <= 0 {
		req.StorageGB = m.cfg.Defaults.StorageGB
	}
	if req.IdleTimeoutSeconds <= 0 {
		req.IdleTimeoutSeconds = m.cfg.Defaults.IdleTimeoutSeconds
	}
	if req.MaxLifetimeSeconds <= 0 {
		req.MaxLifetimeSeconds = m.cfg.Defaults.MaxLifetimeSeconds
	}
	if req.NetworkProfile == "" {
		req.NetworkProfile = m.cfg.Defaults.NetworkProfile
	}
	if req.Env == nil {
		req.Env = map[string]string{}
	}

	image, ok := findImage(m.cfg.Images, req.Image)
	if !ok {
		return CreatePlan{}, newAPIError(http.StatusBadRequest, "IMAGE_NOT_ALLOWED", "image is not in the allowed DevPod image list")
	}
	if len(image.AllowedTags) > 0 && !userHasAnyTag(user, image.AllowedTags) {
		return CreatePlan{}, newAPIError(http.StatusForbidden, "IMAGE_NOT_ALLOWED", "current user is not allowed to use this image")
	}

	profile, ok := findNetworkProfile(m.cfg.NetworkProfiles, req.NetworkProfile)
	if !ok {
		return CreatePlan{}, newAPIError(http.StatusBadRequest, "NETWORK_PROFILE_NOT_ALLOWED", "networkProfile is not configured")
	}
	privileged := userHasAnyTag(user, m.cfg.PrivilegedUserTags)
	if profile.AdminOnly && !privileged {
		return CreatePlan{}, newAPIError(http.StatusForbidden, "NETWORK_PROFILE_FORBIDDEN", "current user is not allowed to use this network profile")
	}
	if req.HostNetwork && !profile.HostNetwork {
		return CreatePlan{}, newAPIError(http.StatusForbidden, "HOST_NETWORK_FORBIDDEN", "hostNetwork must be requested through an allowed hostNetwork profile")
	}
	hostNetwork := req.HostNetwork || profile.HostNetwork
	if hostNetwork && !privileged {
		return CreatePlan{}, newAPIError(http.StatusForbidden, "HOST_NETWORK_FORBIDDEN", "current user is not allowed to use hostNetwork")
	}
	if hostNetwork && len(mergeStringMaps(m.cfg.HostNetworkNodeSelector, profile.NodeSelector)) == 0 {
		return CreatePlan{}, newAPIError(http.StatusBadRequest, "HOST_NETWORK_NODE_SELECTOR_REQUIRED", "hostNetwork profile requires a dedicated node selector")
	}
	mpiEnabled := req.MPIEnabled || profile.MPI
	if mpiEnabled && !imageHasProfile(image, "mpi") && !privileged {
		return CreatePlan{}, newAPIError(http.StatusForbidden, "MPI_PROFILE_FORBIDDEN", "MPI mode requires an MPI-enabled image or a privileged user")
	}
	req.MPIEnabled = mpiEnabled
	req.HostNetwork = hostNetwork

	if req.CPU > m.cfg.Limits.MaxCPUPerPod {
		return CreatePlan{}, newAPIError(http.StatusBadRequest, "RESOURCE_LIMIT_EXCEEDED", "requested CPU exceeds the configured limit")
	}
	if req.MemoryMB > m.cfg.Limits.MaxMemoryMBPerPod {
		return CreatePlan{}, newAPIError(http.StatusBadRequest, "RESOURCE_LIMIT_EXCEEDED", "requested memory exceeds the configured limit")
	}
	if req.GPU > m.cfg.Limits.MaxGPUPerPod {
		return CreatePlan{}, newAPIError(http.StatusBadRequest, "RESOURCE_LIMIT_EXCEEDED", "requested GPU count exceeds the configured limit")
	}
	if req.GPU > 0 && !privileged {
		return CreatePlan{}, newAPIError(http.StatusForbidden, "GPU_FORBIDDEN", "current user is not allowed to request GPU resources")
	}
	if req.StorageGB > m.cfg.Limits.MaxStorageGBPerPod {
		return CreatePlan{}, newAPIError(http.StatusBadRequest, "RESOURCE_LIMIT_EXCEEDED", "requested storage exceeds the configured limit")
	}
	if req.CPU <= 0 || req.MemoryMB <= 0 || req.GPU < 0 || req.StorageGB <= 0 {
		return CreatePlan{}, newAPIError(http.StatusBadRequest, "INVALID_RESOURCE_REQUEST", "resource values must be positive")
	}
	if req.IdleTimeoutSeconds < 0 || req.MaxLifetimeSeconds <= 0 {
		return CreatePlan{}, newAPIError(http.StatusBadRequest, "INVALID_LIFETIME", "invalid DevPod lifetime settings")
	}

	if err := validateEnv(req.Env, m.cfg.Limits.MaxEnvVarsPerPod); err != nil {
		return CreatePlan{}, err
	}
	command := req.Command
	if len(command) == 0 {
		command = append([]string(nil), m.cfg.Defaults.Command...)
	}
	if err := validateCommand(command, m.cfg.Limits.MaxCommandArgs); err != nil {
		return CreatePlan{}, err
	}

	return CreatePlan{
		Request:        req,
		OwnerName:      DevPodOwnerName(user),
		Image:          image,
		NetworkProfile: profile,
		Command:        command,
		Env:            cloneStringMap(req.Env),
		ExpiresAt:      time.Now().Add(time.Duration(req.MaxLifetimeSeconds) * time.Second),
	}, nil
}

func (m *Manager) BuildSession(user models.User, plan CreatePlan) (models.DevPodSession, error) {
	id := uuid.NewString()
	resourceName := ResourceName(id)
	sshUser := fmt.Sprintf("%s+%s", plan.OwnerName, resourceName)
	commandJSON, err := json.Marshal(plan.Command)
	if err != nil {
		return models.DevPodSession{}, err
	}
	return models.DevPodSession{
		ID:                 id,
		UserID:             user.ID,
		Username:           user.Username,
		OwnerName:          plan.OwnerName,
		Name:               resourceName,
		DisplayName:        plan.Request.DisplayName,
		Image:              plan.Request.Image,
		CPU:                plan.Request.CPU,
		MemoryMB:           plan.Request.MemoryMB,
		GPU:                plan.Request.GPU,
		StorageGB:          plan.Request.StorageGB,
		Persistent:         plan.Request.Persistent,
		NetworkMode:        plan.Request.NetworkProfile,
		MPIEnabled:         plan.Request.MPIEnabled,
		HostNetwork:        plan.Request.HostNetwork,
		Status:             models.DevPodStatusPending,
		Namespace:          m.cfg.Namespace,
		K8sResourceName:    resourceName,
		SSHUser:            sshUser,
		SSHHost:            m.cfg.Gateway.Host,
		SSHPort:            m.cfg.Gateway.Port,
		SSHCommand:         sshCommand(sshUser, m.cfg.Gateway.Host, m.cfg.Gateway.Port),
		IdleTimeoutSeconds: plan.Request.IdleTimeoutSeconds,
		MaxLifetimeSeconds: plan.Request.MaxLifetimeSeconds,
		Env:                jsonMapFromStringMap(plan.Env),
		Command:            string(commandJSON),
		ExpiresAt:          plan.ExpiresAt,
	}, nil
}

func ParseAuthorizedKey(raw string) (string, string, error) {
	fields := strings.Fields(raw)
	if len(fields) < 2 {
		return "", "", newAPIError(http.StatusBadRequest, "INVALID_SSH_KEY", "SSH public key must be in OpenSSH authorized_keys format")
	}
	keyType := fields[0]
	if !sshKeyTypeAllowlist[keyType] {
		return "", "", newAPIError(http.StatusBadRequest, "INVALID_SSH_KEY", "unsupported SSH public key type")
	}
	keyBytes, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		return "", "", newAPIError(http.StatusBadRequest, "INVALID_SSH_KEY", "SSH public key is not valid base64")
	}
	fingerprintSum := sha256.Sum256(keyBytes)
	fingerprint := "SHA256:" + base64.RawStdEncoding.EncodeToString(fingerprintSum[:])
	normalized := keyType + " " + fields[1]
	if len(fields) > 2 {
		normalized += " " + strings.Join(fields[2:], " ")
	}
	return normalized, fingerprint, nil
}

func ResourceName(sessionID string) string {
	clean := strings.ReplaceAll(sessionID, "-", "")
	if len(clean) < 12 {
		sum := sha256.Sum256([]byte(sessionID))
		clean = hex.EncodeToString(sum[:])
	}
	return "csoj-" + clean[:12]
}

func DevPodOwnerName(user models.User) string {
	base := strings.ToLower(strings.TrimSpace(user.Username))
	base = devpodUserPattern.ReplaceAllString(base, "-")
	base = strings.Trim(base, "-")
	if base == "" {
		base = "u"
	}
	if len(base) <= 32 {
		return base
	}
	sum := sha256.Sum256([]byte(user.ID + ":" + user.Username))
	suffix := hex.EncodeToString(sum[:])[:8]
	base = strings.Trim(base[:23], "-")
	if base == "" {
		base = "u"
	}
	return base + "-" + suffix
}

func (m *Manager) runKubectl(ctx context.Context, input []byte, namespaced bool, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, m.cfg.Kubectl, m.kubectlArgs(namespaced, args...)...)
	if input != nil {
		command.Stdin = strings.NewReader(string(input))
	}
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("kubectl %s failed: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

func (m *Manager) kubectlArgs(namespaced bool, args ...string) []string {
	base := make([]string, 0, len(args)+8)
	if m.cfg.Kubeconfig != "" {
		base = append(base, "--kubeconfig", m.cfg.Kubeconfig)
	}
	if m.cfg.Context != "" {
		base = append(base, "--context", m.cfg.Context)
	}
	if namespaced && m.cfg.Namespace != "" {
		base = append(base, "-n", m.cfg.Namespace)
	}
	return append(base, args...)
}

func marshalYAMLDocuments(objects ...map[string]interface{}) ([]byte, error) {
	var out strings.Builder
	for i, obj := range objects {
		if i > 0 {
			out.WriteString("---\n")
		}
		data, err := yaml.Marshal(obj)
		if err != nil {
			return nil, err
		}
		out.Write(data)
	}
	return []byte(out.String()), nil
}

func allowedImages(images []config.DevPodImage) []config.DevPodImage {
	out := make([]config.DevPodImage, 0, len(images))
	for _, image := range images {
		if image.Allowed {
			out = append(out, image)
		}
	}
	return out
}

func findImage(images []config.DevPodImage, imageName string) (config.DevPodImage, bool) {
	for _, image := range images {
		if image.Image == imageName && image.Allowed {
			return image, true
		}
	}
	return config.DevPodImage{}, false
}

func findNetworkProfile(profiles []config.DevPodNetworkProfile, name string) (config.DevPodNetworkProfile, bool) {
	for _, profile := range profiles {
		if profile.Name == name {
			return profile, true
		}
	}
	return config.DevPodNetworkProfile{}, false
}

func imageHasProfile(image config.DevPodImage, profile string) bool {
	for _, item := range image.Profiles {
		if strings.EqualFold(item, profile) {
			return true
		}
	}
	return false
}

func userHasAnyTag(user models.User, required []string) bool {
	if len(required) == 0 {
		return true
	}
	tags := map[string]bool{}
	for _, item := range strings.Split(user.Tags, ",") {
		item = strings.ToLower(strings.TrimSpace(item))
		if item != "" {
			tags[item] = true
		}
	}
	for _, item := range required {
		if tags[strings.ToLower(strings.TrimSpace(item))] {
			return true
		}
	}
	return false
}

func validateEnv(env map[string]string, max int) error {
	if len(env) > max {
		return newAPIError(http.StatusBadRequest, "TOO_MANY_ENV_VARS", "too many environment variables")
	}
	for name, value := range env {
		if !envNamePattern.MatchString(name) {
			return newAPIError(http.StatusBadRequest, "INVALID_ENV", "environment variable names must match [A-Za-z_][A-Za-z0-9_]*")
		}
		if len(value) > 4096 {
			return newAPIError(http.StatusBadRequest, "INVALID_ENV", "environment variable values must be at most 4096 bytes")
		}
	}
	return nil
}

func validateCommand(command []string, max int) error {
	if len(command) == 0 {
		return newAPIError(http.StatusBadRequest, "INVALID_COMMAND", "command must not be empty")
	}
	if len(command) > max {
		return newAPIError(http.StatusBadRequest, "INVALID_COMMAND", "command has too many arguments")
	}
	for _, arg := range command {
		if strings.TrimSpace(arg) == "" {
			return newAPIError(http.StatusBadRequest, "INVALID_COMMAND", "command arguments must not be empty")
		}
		if strings.ContainsRune(arg, '\x00') {
			return newAPIError(http.StatusBadRequest, "INVALID_COMMAND", "command arguments must not contain NUL bytes")
		}
	}
	return nil
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func jsonMapFromStringMap(in map[string]string) models.JSONMap {
	out := models.JSONMap{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

func stringMapFromJSONMap(in models.JSONMap) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func sortedEnv(env map[string]string) []map[string]string {
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]map[string]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, map[string]string{"name": key, "value": env[key]})
	}
	return out
}

func mergeStringMaps(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, item := range maps {
		for key, value := range item {
			out[key] = value
		}
	}
	return out
}

func sshCommand(user, host string, port int) string {
	if port > 0 && port != 22 {
		return fmt.Sprintf("ssh -p %d %s@%s", port, user, host)
	}
	return fmt.Sprintf("ssh %s@%s", user, host)
}

func parseCommand(raw string, fallback []string) []string {
	var command []string
	if raw != "" && json.Unmarshal([]byte(raw), &command) == nil && len(command) > 0 {
		return command
	}
	return append([]string(nil), fallback...)
}

func statusFromPhase(phase string, running bool) models.DevPodStatus {
	switch phase {
	case "Running":
		return models.DevPodStatusRunning
	case "Stopped":
		return models.DevPodStatusStopped
	case "Failed":
		return models.DevPodStatusFailed
	case "Pending":
		return models.DevPodStatusCreating
	default:
		if !running {
			return models.DevPodStatusStopped
		}
		return models.DevPodStatusCreating
	}
}

func ErrorResponse(err error) (int, string, string) {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Status, apiErr.Code, apiErr.Message
	}
	return http.StatusInternalServerError, "INTERNAL_ERROR", err.Error()
}

func formatQuantityMB(memoryMB int) string {
	return strconv.Itoa(memoryMB) + "Mi"
}

func formatQuantityGB(storageGB int) string {
	return strconv.Itoa(storageGB) + "Gi"
}
