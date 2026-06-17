package judger

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"go.uber.org/zap"
)

type DockerManager struct {
	cli *client.Client
}

type ExecResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
	Usage    RuntimeUsage
}

type RuntimeUsage struct {
	AveCPU    float64
	AveRSS    int64
	MaxRSS    int64
	MaxVMSize int64
}

func NewDockerManager(cfg config.DockerConfig) (*DockerManager, error) {
	opts := []client.Opt{
		client.WithHost(cfg.Host),
		client.WithAPIVersionNegotiation(),
	}

	if cfg.TLSVerify {
		// client.WithTLSClientConfig can create the http.Client with TLS config
		// from the given paths.
		tlsOpts := client.WithTLSClientConfig(cfg.CACert, cfg.Cert, cfg.Key)
		opts = append(opts, tlsOpts)
	}

	cli, err := client.NewClientWithOpts(opts...)
	if err != nil {
		return nil, err
	}
	return &DockerManager{cli: cli}, nil
}

func (m *DockerManager) CreateVolume(name string) error {
	_, err := m.cli.VolumeCreate(context.Background(), volume.CreateOptions{
		Name: name,
	})
	return err
}

func (m *DockerManager) RemoveVolume(name string) error {
	// The 'force' parameter allows removing the volume even if it's in use by a stopped container.
	return m.cli.VolumeRemove(context.Background(), name, true)
}

func (m *DockerManager) CreateContainer(image, volumeName string, cpu int, cpusetCpus string, memory int64, asRoot bool, customMounts []Mount, networkEnabled bool, name string, envs []string) (string, error) {
	ctx := context.Background()

	config := &container.Config{
		Image:           image,
		Tty:             false, // Tty must be false to multiplex stdout/stderr
		OpenStdin:       true,
		AttachStdin:     true,
		AttachStdout:    true,
		AttachStderr:    true,
		NetworkDisabled: !networkEnabled,
		Env:             envs,
	}

	if !asRoot {
		config.User = "1000:1000"
	}

	// Initialize dockerMounts with the main submission volume
	dockerMounts := []mount.Mount{
		{
			Type:   mount.TypeVolume,
			Source: volumeName,
			Target: "/mnt/work",
		},
	}

	hostConfig := &container.HostConfig{
		Resources: container.Resources{
			NanoCPUs:   int64(cpu) * 1e9,
			Memory:     memory * 1024 * 1024,
			CpusetCpus: cpusetCpus,
		},
	}

	// Append custom mounts from problem.yaml
	for _, mnt := range customMounts {
		mountType := mount.TypeBind
		if mnt.Type != "" {
			mountType = mount.Type(mnt.Type)
		}

		isReadOnly := true // Default to true
		if mnt.ReadOnly != nil {
			isReadOnly = *mnt.ReadOnly
		}

		var tmpfsOptions *mount.TmpfsOptions
		tmpfsOptions = nil
		if mountType == mount.TypeTmpfs {
			tmpfsOptions = &mount.TmpfsOptions{
				SizeBytes: mnt.TmpfsOption.SizeBytes,
				Mode:      mnt.TmpfsOption.Mode,
				Options:   mnt.TmpfsOption.Options,
			}
		}

		dockerMounts = append(dockerMounts, mount.Mount{
			Type:         mountType,
			Source:       mnt.Source,
			Target:       mnt.Target,
			ReadOnly:     isReadOnly,
			TmpfsOptions: tmpfsOptions,
		})
	}
	hostConfig.Mounts = dockerMounts

	resp, err := m.cli.ContainerCreate(ctx, config, hostConfig, nil, nil, name)
	if err != nil {
		return "", err
	}

	return resp.ID, nil
}

func (m *DockerManager) StartContainer(containerID string) error {
	return m.cli.ContainerStart(context.Background(), containerID, container.StartOptions{})
}

func (m *DockerManager) ExecInContainer(ctx context.Context, containerID string, cmd []string, outputCallback func(streamType string, data []byte)) (ExecResult, error) {
	sampler := newDockerUsageSampler(m, containerID)
	sampler.Start(ctx)
	defer sampler.Stop()

	execConfig := container.ExecOptions{
		Cmd:          cmd,
		AttachStdout: true,
		AttachStderr: true,
	}

	execCreateResp, err := m.cli.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return ExecResult{}, err
	}
	execID := execCreateResp.ID

	resp, err := m.cli.ContainerExecAttach(ctx, execID, container.ExecAttachOptions{})
	if err != nil {
		return ExecResult{}, err
	}
	defer resp.Close()

	var stdoutBuf, stderrBuf bytes.Buffer
	stdoutWriter := newCallbackWriter("stdout", &stdoutBuf, outputCallback)
	stderrWriter := newCallbackWriter("stderr", &stderrBuf, outputCallback)

	_, err = stdcopy.StdCopy(stdoutWriter, stderrWriter, resp.Reader)
	if err != nil {
		zap.S().Warnf("error copying stdout/stderr from container exec: %v", err)
	}

	var inspect container.ExecInspect
	for {
		if ctx.Err() != nil {
			return ExecResult{Usage: sampler.Usage()}, ctx.Err()
		}

		inspect, err = m.cli.ContainerExecInspect(ctx, execID)
		if err != nil {
			return ExecResult{Usage: sampler.Usage()}, err
		}
		if !inspect.Running {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return ExecResult{
		Stdout:   stdoutBuf.String(),
		Stderr:   stderrBuf.String(),
		ExitCode: inspect.ExitCode,
		Usage:    sampler.Usage(),
	}, nil
}

type dockerUsageSampler struct {
	manager     *DockerManager
	containerID string
	done        chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
	mu          sync.Mutex
	samples     []dockerUsageSample
}

type dockerUsageSample struct {
	cpuTotal uint64
	memory   uint64
	maxRSS   uint64
	vmSize   uint64
}

func newDockerUsageSampler(manager *DockerManager, containerID string) *dockerUsageSampler {
	return &dockerUsageSampler{
		manager:     manager,
		containerID: containerID,
		done:        make(chan struct{}),
	}
}

func (s *dockerUsageSampler) Start(ctx context.Context) {
	if s == nil || s.manager == nil || strings.TrimSpace(s.containerID) == "" {
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

func (s *dockerUsageSampler) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.done)
	})
	s.wg.Wait()
	s.capture(context.Background())
}

func (s *dockerUsageSampler) Usage() RuntimeUsage {
	if s == nil {
		return RuntimeUsage{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.samples) == 0 {
		return RuntimeUsage{}
	}
	first := s.samples[0]
	last := s.samples[len(s.samples)-1]
	var totalMemory uint64
	var maxRSS uint64
	var maxVMSize uint64
	for _, sample := range s.samples {
		totalMemory += sample.memory
		if sample.maxRSS > maxRSS {
			maxRSS = sample.maxRSS
		}
		if sample.vmSize > maxVMSize {
			maxVMSize = sample.vmSize
		}
	}
	usage := RuntimeUsage{
		AveRSS:    int64(totalMemory / uint64(len(s.samples))),
		MaxRSS:    int64(maxRSS),
		MaxVMSize: int64(maxVMSize),
	}
	if last.cpuTotal > first.cpuTotal {
		usage.AveCPU = float64(last.cpuTotal-first.cpuTotal) / 1e9
	}
	return usage
}

func (s *dockerUsageSampler) capture(ctx context.Context) {
	if s == nil || s.manager == nil {
		return
	}
	stats, err := s.manager.containerStats(ctx, s.containerID)
	if err != nil {
		zap.S().Debugf("failed to sample docker stats for %s: %v", s.containerID, err)
		return
	}
	sample := dockerStatsSample(stats)
	s.mu.Lock()
	s.samples = append(s.samples, sample)
	s.mu.Unlock()
}

func (m *DockerManager) containerStats(ctx context.Context, containerID string) (container.StatsResponse, error) {
	statsReader, err := m.cli.ContainerStatsOneShot(ctx, containerID)
	if err != nil {
		return container.StatsResponse{}, err
	}
	defer statsReader.Body.Close()
	var stats container.StatsResponse
	if err := json.NewDecoder(statsReader.Body).Decode(&stats); err != nil {
		return container.StatsResponse{}, err
	}
	return stats, nil
}

func dockerStatsSample(stats container.StatsResponse) dockerUsageSample {
	memory := stats.MemoryStats.Usage
	if memory == 0 {
		memory = stats.MemoryStats.PrivateWorkingSet
	}
	maxRSS := stats.MemoryStats.MaxUsage
	if maxRSS == 0 {
		maxRSS = stats.MemoryStats.CommitPeak
	}
	if maxRSS == 0 {
		maxRSS = memory
	}
	vmSize := stats.MemoryStats.Commit
	if vmSize == 0 {
		vmSize = stats.MemoryStats.Usage
	}
	if vmSize == 0 {
		vmSize = stats.MemoryStats.PrivateWorkingSet
	}
	return dockerUsageSample{
		cpuTotal: stats.CPUStats.CPUUsage.TotalUsage,
		memory:   memory,
		maxRSS:   maxRSS,
		vmSize:   vmSize,
	}
}

func (m *DockerManager) PauseContainer(containerID string) error {
	return m.cli.ContainerPause(context.Background(), containerID)
}

func (m *DockerManager) ResumeContainer(containerID string) error {
	return m.cli.ContainerUnpause(context.Background(), containerID)
}

func (m *DockerManager) SignalContainer(containerID string, signal string) error {
	return m.cli.ContainerKill(context.Background(), containerID, NormalizeSignal(signal))
}

// an io.Writer that calls a callback function and writes to a buffer.
type callbackWriter struct {
	streamType string
	buffer     *bytes.Buffer
	callback   func(streamType string, data []byte)
}

func newCallbackWriter(streamType string, buffer *bytes.Buffer, callback func(string, []byte)) *callbackWriter {
	return &callbackWriter{
		streamType: streamType,
		buffer:     buffer,
		callback:   callback,
	}
}

func (w *callbackWriter) Write(p []byte) (int, error) {
	w.callback(w.streamType, p)
	return w.buffer.Write(p)
}

func (m *DockerManager) CleanupContainer(containerID string) {
	ctx := context.Background()

	_, err := m.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		// Container might already be removed
		zap.S().Warnf("failed to inspect container %s before cleanup: %v", containerID, err)
		return
	}

	timeoutSeconds := 0
	stopOptions := container.StopOptions{Timeout: &timeoutSeconds}
	if err := m.cli.ContainerStop(ctx, containerID, stopOptions); err != nil {
		zap.S().Warnf("failed to stop container %s: %v", containerID, err)
	}

	removeOptions := container.RemoveOptions{Force: true}
	if err := m.cli.ContainerRemove(ctx, containerID, removeOptions); err != nil {
		zap.S().Warnf("failed to remove container %s: %v", containerID, err)
		return
	}

	zap.S().Infof("cleaned up container %s", containerID)
}

func (m *DockerManager) CopyToContainer(containerID string, srcDir string, dstDir string) error {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}

		fr, err := os.Open(path)
		if err != nil {
			return err
		}
		defer fr.Close()

		hdr := &tar.Header{
			Name: relPath,
			Mode: 0644,
			Size: info.Size(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := io.Copy(tw, fr); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to walk source directory: %w", err)
	}

	if err := tw.Close(); err != nil {
		return fmt.Errorf("failed to close tar writer: %w", err)
	}

	tarReader := bytes.NewReader(buf.Bytes())
	return m.cli.CopyToContainer(context.Background(), containerID, dstDir, tarReader, container.CopyToContainerOptions{})
}
