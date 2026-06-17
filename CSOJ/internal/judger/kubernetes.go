package judger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

const (
	defaultKubernetesNamespace       = "default"
	defaultKubernetesKubectl         = "kubectl"
	defaultKubernetesRunnerContainer = "runner"
	defaultKubernetesWorkdirSize     = "1Gi"
	defaultKubernetesStartupTimeout  = 120
)

type KubernetesManager struct {
	cfg config.KubernetesConfig
}

func NewKubernetesManager(cfg config.KubernetesConfig) (*KubernetesManager, error) {
	if cfg.Kubectl == "" {
		cfg.Kubectl = defaultKubernetesKubectl
	}
	if cfg.Namespace == "" {
		cfg.Namespace = defaultKubernetesNamespace
	}
	if cfg.WorkdirSize == "" {
		cfg.WorkdirSize = defaultKubernetesWorkdirSize
	}
	if cfg.StartupTimeoutSeconds <= 0 {
		cfg.StartupTimeoutSeconds = defaultKubernetesStartupTimeout
	}
	if cfg.RunnerContainerName == "" {
		cfg.RunnerContainerName = defaultKubernetesRunnerContainer
	}
	if len(cfg.RunnerCommand) == 0 {
		cfg.RunnerCommand = []string{"/bin/sh", "-c", "trap : TERM INT; sleep 3650d & wait"}
	}
	return &KubernetesManager{cfg: cfg}, nil
}

func (m *KubernetesManager) CreateVolume(name string) error {
	manifest := m.buildPVCManifest(name)
	data, err := yaml.Marshal(manifest)
	if err != nil {
		return err
	}
	return m.runKubectlInput(context.Background(), data, "apply", "-f", "-")
}

func (m *KubernetesManager) RemoveVolume(name string) error {
	return m.runKubectl(context.Background(), "delete", "pvc", m.volumeName(name), "--ignore-not-found=true")
}

func (m *KubernetesManager) CreateContainer(image, volumeName string, cpu int, cpusetCpus string, memory int64, asRoot bool, customMounts []Mount, networkEnabled bool, name string, envs []string) (string, error) {
	podName := kubernetesName(name)
	manifest := m.buildPodManifest(podName, image, volumeName, cpu, memory, asRoot, customMounts, networkEnabled, envs)
	data, err := yaml.Marshal(manifest)
	if err != nil {
		return "", err
	}
	if err := m.runKubectlInput(context.Background(), data, "apply", "-f", "-"); err != nil {
		return "", err
	}
	return podName, nil
}

func (m *KubernetesManager) StartContainer(containerID string) error {
	timeout := fmt.Sprintf("%ds", m.cfg.StartupTimeoutSeconds)
	return m.runKubectl(context.Background(), "wait", "--for=condition=Ready", "pod/"+containerID, "--timeout="+timeout)
}

func (m *KubernetesManager) ExecInContainer(ctx context.Context, containerID string, cmd []string, outputCallback func(streamType string, data []byte)) (ExecResult, error) {
	sampler := newKubernetesUsageSampler(m, containerID)
	sampler.Start(ctx)
	defer sampler.Stop()

	args := []string{"exec", containerID, "-c", m.cfg.RunnerContainerName, "--"}
	args = append(args, cmd...)
	command := exec.CommandContext(ctx, m.cfg.Kubectl, m.kubectlArgs(args...)...)

	stdoutPipe, err := command.StdoutPipe()
	if err != nil {
		return ExecResult{}, err
	}
	stderrPipe, err := command.StderrPipe()
	if err != nil {
		return ExecResult{}, err
	}

	if err := command.Start(); err != nil {
		return ExecResult{}, err
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(newCallbackWriter("stdout", &stdoutBuf, outputCallback), stdoutPipe)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(newCallbackWriter("stderr", &stderrBuf, outputCallback), stderrPipe)
	}()

	waitErr := command.Wait()
	wg.Wait()

	result := ExecResult{
		Stdout:   stdoutBuf.String(),
		Stderr:   stderrBuf.String(),
		ExitCode: 0,
		Usage:    sampler.Usage(),
	}
	if ctx.Err() != nil {
		result.ExitCode = -1
		result.Usage = sampler.Usage()
		return result, ctx.Err()
	}
	if waitErr != nil {
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
			result.Usage = sampler.Usage()
			return result, nil
		}
		result.ExitCode = -1
		result.Usage = sampler.Usage()
		return result, waitErr
	}
	result.Usage = sampler.Usage()
	return result, nil
}

func (m *KubernetesManager) PauseContainer(containerID string) error {
	return m.signalContainerProcesses(containerID, "SIGSTOP")
}

func (m *KubernetesManager) ResumeContainer(containerID string) error {
	return m.signalContainerProcesses(containerID, "SIGCONT")
}

func (m *KubernetesManager) SignalContainer(containerID string, signal string) error {
	normalized := NormalizeSignal(signal)
	if err := m.signalContainerProcesses(containerID, normalized); err != nil {
		return err
	}
	if normalized == "SIGKILL" {
		m.CleanupContainer(containerID)
	}
	return nil
}

func (m *KubernetesManager) CleanupContainer(containerID string) {
	if err := m.runKubectl(context.Background(), "delete", "pod", containerID, "--ignore-not-found=true", "--grace-period=0", "--force"); err != nil {
		zap.S().Warnf("failed to delete kubernetes pod %s: %v", containerID, err)
	}
}

func (m *KubernetesManager) CopyToContainer(containerID string, srcDir string, dstDir string) error {
	src := filepath.Join(filepath.Clean(srcDir), ".")
	dst := fmt.Sprintf("%s/%s:%s", m.cfg.Namespace, containerID, dstDir)
	return m.runKubectl(context.Background(), "cp", src, dst, "-c", m.cfg.RunnerContainerName)
}

func (m *KubernetesManager) signalContainerProcesses(containerID, signal string) error {
	token, err := kubernetesSignalToken(signal)
	if err != nil {
		return err
	}
	if strings.TrimSpace(containerID) == "" {
		return fmt.Errorf("kubernetes pod name is required")
	}
	return m.runKubectl(context.Background(), kubernetesSignalArgs(containerID, m.cfg.RunnerContainerName, token)...)
}

func kubernetesSignalArgs(containerID, containerName, signalToken string) []string {
	return []string{
		"exec", containerID,
		"-c", containerName,
		"--", "/bin/sh", "-c",
		kubernetesSignalScript(signalToken),
	}
}

func kubernetesSignalScript(signalToken string) string {
	return fmt.Sprintf(`for p in /proc/[0-9]*; do pid=${p##*/}; case "$pid" in 1|$$) continue;; esac; kill -s %s "$pid" 2>/dev/null || true; done`, signalToken)
}

func kubernetesSignalToken(signal string) (string, error) {
	normalized := NormalizeSignal(signal)
	token := strings.TrimPrefix(normalized, "SIG")
	if token == "" {
		return "", fmt.Errorf("invalid signal %q", signal)
	}
	for _, ch := range token {
		if (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') {
			return "", fmt.Errorf("invalid signal %q", signal)
		}
	}
	return token, nil
}

func (m *KubernetesManager) buildPVCManifest(name string) map[string]interface{} {
	spec := map[string]interface{}{
		"accessModes": []string{"ReadWriteOnce"},
		"resources": map[string]interface{}{
			"requests": map[string]string{
				"storage": m.cfg.WorkdirSize,
			},
		},
	}
	if m.cfg.StorageClassName != "" {
		spec["storageClassName"] = m.cfg.StorageClassName
	}
	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata": map[string]interface{}{
			"name":      m.volumeName(name),
			"namespace": m.cfg.Namespace,
			"labels":    kubernetesLabels(name),
		},
		"spec": spec,
	}
}

func (m *KubernetesManager) buildPodManifest(podName, image, volumeName string, cpu int, memory int64, asRoot bool, customMounts []Mount, networkEnabled bool, envs []string) map[string]interface{} {
	volumes := []map[string]interface{}{
		{
			"name": "workdir",
			"persistentVolumeClaim": map[string]string{
				"claimName": m.volumeName(volumeName),
			},
		},
	}
	volumeMounts := []map[string]interface{}{
		{
			"name":      "workdir",
			"mountPath": "/mnt/work",
		},
	}
	for i, mount := range customMounts {
		volume, volumeMount := kubernetesCustomMount(i, mount)
		volumes = append(volumes, volume)
		volumeMounts = append(volumeMounts, volumeMount)
	}

	containerSpec := map[string]interface{}{
		"name":            m.cfg.RunnerContainerName,
		"image":           image,
		"command":         m.cfg.RunnerCommand,
		"env":             kubernetesEnv(envs),
		"volumeMounts":    volumeMounts,
		"imagePullPolicy": "IfNotPresent",
		"resources": map[string]interface{}{
			"requests": kubernetesResourceList(cpu, memory),
			"limits":   kubernetesResourceList(cpu, memory),
		},
	}
	if !asRoot {
		containerSpec["securityContext"] = map[string]interface{}{
			"runAsUser":                1000,
			"runAsGroup":               1000,
			"allowPrivilegeEscalation": false,
		}
	}

	podSpec := map[string]interface{}{
		"restartPolicy":                "Never",
		"containers":                   []map[string]interface{}{containerSpec},
		"volumes":                      volumes,
		"automountServiceAccountToken": false,
	}
	if m.cfg.ServiceAccount != "" {
		podSpec["serviceAccountName"] = m.cfg.ServiceAccount
	}
	if len(m.cfg.ImagePullSecrets) > 0 {
		podSpec["imagePullSecrets"] = kubernetesLocalObjectRefs(m.cfg.ImagePullSecrets)
	}
	if len(m.cfg.NodeSelector) > 0 {
		podSpec["nodeSelector"] = m.cfg.NodeSelector
	}
	if len(m.cfg.Tolerations) > 0 {
		podSpec["tolerations"] = kubernetesTolerations(m.cfg.Tolerations)
	}
	if m.cfg.PriorityClassName != "" {
		podSpec["priorityClassName"] = m.cfg.PriorityClassName
	}
	if m.cfg.RuntimeClassName != "" {
		podSpec["runtimeClassName"] = m.cfg.RuntimeClassName
	}

	annotations := map[string]string{
		"csoj.zjusc.org/network": strconv.FormatBool(networkEnabled),
	}

	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name":        podName,
			"namespace":   m.cfg.Namespace,
			"labels":      kubernetesLabels(volumeName),
			"annotations": annotations,
		},
		"spec": podSpec,
	}
}

func (m *KubernetesManager) runKubectl(ctx context.Context, args ...string) error {
	return m.runKubectlInput(ctx, nil, args...)
}

func (m *KubernetesManager) runKubectlOutput(ctx context.Context, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, m.cfg.Kubectl, m.kubectlArgs(args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("kubectl %s failed: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

func (m *KubernetesManager) runKubectlInput(ctx context.Context, input []byte, args ...string) error {
	command := exec.CommandContext(ctx, m.cfg.Kubectl, m.kubectlArgs(args...)...)
	if input != nil {
		command.Stdin = bytes.NewReader(input)
	}
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl %s failed: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (m *KubernetesManager) kubectlArgs(args ...string) []string {
	base := make([]string, 0, len(args)+6)
	if m.cfg.Kubeconfig != "" {
		base = append(base, "--kubeconfig", m.cfg.Kubeconfig)
	}
	if m.cfg.Context != "" {
		base = append(base, "--context", m.cfg.Context)
	}
	if m.cfg.Namespace != "" {
		base = append(base, "-n", m.cfg.Namespace)
	}
	return append(base, args...)
}

type kubernetesUsageSampler struct {
	manager *KubernetesManager
	podName string
	done    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
	mu      sync.Mutex
	samples []kubernetesUsageSample
}

type kubernetesUsageSample struct {
	at          time.Time
	cpuCores    float64
	memoryBytes int64
}

func newKubernetesUsageSampler(manager *KubernetesManager, podName string) *kubernetesUsageSampler {
	return &kubernetesUsageSampler{
		manager: manager,
		podName: podName,
		done:    make(chan struct{}),
	}
}

func (s *kubernetesUsageSampler) Start(ctx context.Context) {
	if s == nil || s.manager == nil || strings.TrimSpace(s.podName) == "" {
		return
	}
	s.capture(ctx)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.done:
				return
			case <-ticker.C:
				s.capture(ctx)
			}
		}
	}()
}

func (s *kubernetesUsageSampler) Stop() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		close(s.done)
	})
	s.wg.Wait()
	s.capture(context.Background())
}

func (s *kubernetesUsageSampler) Usage() RuntimeUsage {
	if s == nil {
		return RuntimeUsage{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.samples) == 0 {
		return RuntimeUsage{}
	}
	var totalCPU float64
	var totalMemory int64
	var maxMemory int64
	for _, sample := range s.samples {
		totalCPU += sample.cpuCores
		totalMemory += sample.memoryBytes
		if sample.memoryBytes > maxMemory {
			maxMemory = sample.memoryBytes
		}
	}
	usage := RuntimeUsage{
		AveRSS:    totalMemory / int64(len(s.samples)),
		MaxRSS:    maxMemory,
		MaxVMSize: maxMemory,
	}
	if len(s.samples) > 1 {
		elapsed := s.samples[len(s.samples)-1].at.Sub(s.samples[0].at).Seconds()
		if elapsed > 0 {
			usage.AveCPU = (totalCPU / float64(len(s.samples))) * elapsed
		}
	}
	return usage
}

func (s *kubernetesUsageSampler) capture(ctx context.Context) {
	if s == nil || s.manager == nil {
		return
	}
	output, err := s.manager.runKubectlOutput(ctx, "top", "pod", s.podName, "--no-headers")
	if err != nil {
		zap.S().Debugf("failed to sample kubernetes metrics for pod %s: %v", s.podName, err)
		return
	}
	sample, err := kubernetesUsageSampleFromTopOutput(output, time.Now())
	if err != nil {
		zap.S().Debugf("failed to parse kubernetes metrics for pod %s: %v", s.podName, err)
		return
	}
	s.mu.Lock()
	s.samples = append(s.samples, sample)
	s.mu.Unlock()
}

func kubernetesUsageSampleFromTopOutput(output []byte, at time.Time) (kubernetesUsageSample, error) {
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		cpu, err := parseKubernetesCPU(fields[len(fields)-2])
		if err != nil {
			return kubernetesUsageSample{}, err
		}
		memory, err := parseKubernetesMemoryBytes(fields[len(fields)-1])
		if err != nil {
			return kubernetesUsageSample{}, err
		}
		return kubernetesUsageSample{at: at, cpuCores: cpu, memoryBytes: memory}, nil
	}
	return kubernetesUsageSample{}, fmt.Errorf("no pod metrics in kubectl top output")
}

func parseKubernetesCPU(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty CPU value")
	}
	multipliers := map[string]float64{
		"n": 1e-9,
		"u": 1e-6,
		"m": 1e-3,
	}
	for suffix, multiplier := range multipliers {
		if strings.HasSuffix(value, suffix) {
			amount, err := strconv.ParseFloat(strings.TrimSuffix(value, suffix), 64)
			if err != nil {
				return 0, err
			}
			return amount * multiplier, nil
		}
	}
	return strconv.ParseFloat(value, 64)
}

func parseKubernetesMemoryBytes(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty memory value")
	}
	units := []struct {
		suffix     string
		multiplier int64
	}{
		{"Ki", 1024},
		{"Mi", 1024 * 1024},
		{"Gi", 1024 * 1024 * 1024},
		{"Ti", 1024 * 1024 * 1024 * 1024},
		{"K", 1000},
		{"M", 1000 * 1000},
		{"G", 1000 * 1000 * 1000},
		{"T", 1000 * 1000 * 1000 * 1000},
	}
	for _, unit := range units {
		if strings.HasSuffix(value, unit.suffix) {
			amount, err := strconv.ParseFloat(strings.TrimSuffix(value, unit.suffix), 64)
			if err != nil {
				return 0, err
			}
			return int64(amount * float64(unit.multiplier)), nil
		}
	}
	return strconv.ParseInt(value, 10, 64)
}

func (m *KubernetesManager) volumeName(name string) string {
	return kubernetesName("csoj-work-" + name)
}

func kubernetesCustomMount(index int, mount Mount) (map[string]interface{}, map[string]interface{}) {
	name := fmt.Sprintf("custom-%d", index)
	readOnly := true
	if mount.ReadOnly != nil {
		readOnly = *mount.ReadOnly
	}
	volume := map[string]interface{}{"name": name}
	switch strings.ToLower(strings.TrimSpace(mount.Type)) {
	case "tmpfs":
		emptyDir := map[string]interface{}{"medium": "Memory"}
		if mount.TmpfsOption.SizeBytes > 0 {
			emptyDir["sizeLimit"] = strconv.FormatInt(mount.TmpfsOption.SizeBytes, 10)
		}
		volume["emptyDir"] = emptyDir
	case "volume", "pvc", "persistentvolumeclaim", "persistent_volume_claim":
		volume["persistentVolumeClaim"] = map[string]string{"claimName": mount.Source}
	default:
		volume["hostPath"] = map[string]string{"path": mount.Source}
	}
	volumeMount := map[string]interface{}{
		"name":      name,
		"mountPath": mount.Target,
		"readOnly":  readOnly,
	}
	return volume, volumeMount
}

func kubernetesEnv(envs []string) []map[string]string {
	out := make([]map[string]string, 0, len(envs))
	for _, env := range envs {
		name, value, ok := strings.Cut(env, "=")
		if !ok || name == "" {
			continue
		}
		out = append(out, map[string]string{"name": name, "value": value})
	}
	return out
}

func kubernetesResourceList(cpu int, memory int64) map[string]string {
	resources := make(map[string]string)
	if cpu > 0 {
		resources["cpu"] = strconv.Itoa(cpu)
	}
	if memory > 0 {
		resources["memory"] = fmt.Sprintf("%dMi", memory)
	}
	return resources
}

func kubernetesLocalObjectRefs(names []string) []map[string]string {
	refs := make([]map[string]string, 0, len(names))
	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			continue
		}
		refs = append(refs, map[string]string{"name": name})
	}
	return refs
}

func kubernetesTolerations(in []config.KubernetesToleration) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(in))
	for _, item := range in {
		toleration := make(map[string]interface{})
		if item.Key != "" {
			toleration["key"] = item.Key
		}
		if item.Operator != "" {
			toleration["operator"] = item.Operator
		}
		if item.Value != "" {
			toleration["value"] = item.Value
		}
		if item.Effect != "" {
			toleration["effect"] = item.Effect
		}
		if item.TolerationSeconds != nil {
			toleration["tolerationSeconds"] = *item.TolerationSeconds
		}
		out = append(out, toleration)
	}
	return out
}

func kubernetesLabels(name string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "csoj-judger",
		"app.kubernetes.io/managed-by": "csoj",
		"csoj.zjusc.org/submission":    kubernetesLabelValue(name),
	}
}

var kubernetesNamePattern = regexp.MustCompile(`[^a-z0-9-]+`)

func kubernetesName(name string) string {
	name = strings.ToLower(name)
	name = kubernetesNamePattern.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	if name == "" {
		name = "csoj"
	}
	if len(name) > 63 {
		name = strings.Trim(name[:63], "-")
	}
	return name
}

func kubernetesLabelValue(value string) string {
	value = kubernetesName(value)
	if len(value) > 63 {
		value = value[:63]
	}
	return value
}

var _ RuntimeManager = (*KubernetesManager)(nil)
