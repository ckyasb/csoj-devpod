package judger

import (
	"context"
	"fmt"
	"strings"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
)

const (
	RuntimeDocker     = "docker"
	RuntimeKubernetes = "kubernetes"
)

type RuntimeManager interface {
	CreateVolume(name string) error
	RemoveVolume(name string) error
	CreateContainer(image, volumeName string, cpu int, cpusetCpus string, memory int64, asRoot bool, customMounts []Mount, networkEnabled bool, name string, envs []string) (string, error)
	StartContainer(containerID string) error
	ExecInContainer(ctx context.Context, containerID string, cmd []string, outputCallback func(streamType string, data []byte)) (ExecResult, error)
	PauseContainer(containerID string) error
	ResumeContainer(containerID string) error
	SignalContainer(containerID string, signal string) error
	CleanupContainer(containerID string)
	CopyToContainer(containerID string, srcDir string, dstDir string) error
}

func NewRuntimeManager(node config.Node) (RuntimeManager, error) {
	switch normalizeRuntime(node.Runtime) {
	case RuntimeDocker:
		return NewDockerManager(node.Docker)
	case RuntimeKubernetes:
		return NewKubernetesManager(node.Kubernetes)
	default:
		return nil, fmt.Errorf("unsupported node runtime %q", node.Runtime)
	}
}

func NodeRuntimeName(node config.Node) string {
	return normalizeRuntime(node.Runtime)
}

func CleanupRuntimeContainers(cfg *config.Config, clusterName, nodeName string, containers []models.Container) error {
	hasRuntimeIDs := false
	for _, container := range containers {
		if container.DockerID != "" {
			hasRuntimeIDs = true
			break
		}
	}
	if !hasRuntimeIDs {
		return nil
	}

	node, ok := FindNodeConfig(cfg, clusterName, nodeName)
	if !ok {
		return fmt.Errorf("node config %q/%q not found", clusterName, nodeName)
	}
	manager, err := NewRuntimeManager(node)
	if err != nil {
		return err
	}
	for _, container := range containers {
		if container.DockerID == "" {
			continue
		}
		manager.CleanupContainer(container.DockerID)
	}
	return nil
}

func SetRuntimeContainersPaused(cfg *config.Config, clusterName, nodeName string, containers []models.Container, paused bool) error {
	hasRuntimeIDs := false
	for _, container := range containers {
		if container.DockerID != "" {
			hasRuntimeIDs = true
			break
		}
	}
	if !hasRuntimeIDs {
		return nil
	}

	node, ok := FindNodeConfig(cfg, clusterName, nodeName)
	if !ok {
		return fmt.Errorf("node config %q/%q not found", clusterName, nodeName)
	}
	manager, err := NewRuntimeManager(node)
	if err != nil {
		return err
	}
	for _, container := range containers {
		if container.DockerID == "" {
			continue
		}
		if paused {
			if err := manager.PauseContainer(container.DockerID); err != nil {
				return err
			}
		} else if err := manager.ResumeContainer(container.DockerID); err != nil {
			return err
		}
	}
	return nil
}

func SignalRuntimeContainers(cfg *config.Config, clusterName, nodeName string, containers []models.Container, signal string) error {
	hasRuntimeIDs := false
	for _, container := range containers {
		if container.DockerID != "" {
			hasRuntimeIDs = true
			break
		}
	}
	if !hasRuntimeIDs {
		return nil
	}

	node, ok := FindNodeConfig(cfg, clusterName, nodeName)
	if !ok {
		return fmt.Errorf("node config %q/%q not found", clusterName, nodeName)
	}
	manager, err := NewRuntimeManager(node)
	if err != nil {
		return err
	}
	for _, container := range containers {
		if container.DockerID == "" {
			continue
		}
		if err := manager.SignalContainer(container.DockerID, signal); err != nil {
			return err
		}
	}
	return nil
}

func FindNodeConfig(cfg *config.Config, clusterName, nodeName string) (config.Node, bool) {
	if cfg == nil {
		return config.Node{}, false
	}
	if nodes := splitNodeList(nodeName); len(nodes) > 0 {
		nodeName = nodes[0]
	}
	for _, cluster := range cfg.Cluster {
		if cluster.Name != clusterName {
			continue
		}
		for _, node := range cluster.Nodes {
			if node.Name == nodeName {
				return node, true
			}
		}
	}
	return config.Node{}, false
}

func NormalizeSignal(signal string) string {
	normalized := strings.ToUpper(strings.TrimSpace(signal))
	if normalized == "" {
		normalized = "TERM"
	}
	if strings.HasPrefix(normalized, "SIG") {
		return normalized
	}
	return "SIG" + normalized
}

func normalizeRuntime(runtime string) string {
	switch strings.ToLower(strings.TrimSpace(runtime)) {
	case "", RuntimeDocker:
		return RuntimeDocker
	case "k8s", RuntimeKubernetes:
		return RuntimeKubernetes
	default:
		return strings.ToLower(strings.TrimSpace(runtime))
	}
}
