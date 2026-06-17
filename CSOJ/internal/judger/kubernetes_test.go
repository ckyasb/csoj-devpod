package judger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"gopkg.in/yaml.v3"
)

func TestKubernetesManagerBuildsPodManifest(t *testing.T) {
	readOnly := false
	manager, err := NewKubernetesManager(config.KubernetesConfig{
		Namespace:         "judge",
		StorageClassName:  "fast",
		NodeSelector:      map[string]string{"pool": "judge"},
		PriorityClassName: "high",
		RunnerCommand:     []string{"/runner"},
	})
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}

	pod := manager.buildPodManifest(
		"sub-0",
		"golang:1.24",
		"submission-1",
		2,
		1024,
		false,
		[]Mount{{Type: "volume", Source: "datasets", Target: "/data", ReadOnly: &readOnly}},
		false,
		[]string{"CSOJ_SUBMIT_DIR=/mnt/work", "SLURM_ARRAY_TASK_ID=3"},
	)
	data, err := yaml.Marshal(pod)
	if err != nil {
		t.Fatalf("marshal pod: %v", err)
	}
	body := string(data)
	for _, want := range []string{
		"namespace: judge",
		"image: golang:1.24",
		"claimName: csoj-work-submission-1",
		"cpu: \"2\"",
		"memory: 1024Mi",
		"name: SLURM_ARRAY_TASK_ID",
		"value: \"3\"",
		"priorityClassName: high",
		"pool: judge",
		"persistentVolumeClaim:",
		"claimName: datasets",
		"csoj.zjusc.org/network: \"false\"",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("manifest missing %q:\n%s", want, body)
		}
	}
}

func TestKubernetesUsageParsesTopOutputAndAggregates(t *testing.T) {
	first := time.Now()
	sample, err := kubernetesUsageSampleFromTopOutput([]byte("runner-pod 250m 64Mi\n"), first)
	if err != nil {
		t.Fatalf("parse top output: %v", err)
	}
	if sample.cpuCores != 0.25 || sample.memoryBytes != 64*1024*1024 {
		t.Fatalf("unexpected sample: %#v", sample)
	}

	sampler := &kubernetesUsageSampler{
		samples: []kubernetesUsageSample{
			sample,
			{at: first.Add(2 * time.Second), cpuCores: 0.75, memoryBytes: 128 * 1024 * 1024},
		},
	}
	usage := sampler.Usage()
	if usage.AveCPU != 1.0 || usage.AveRSS != 96*1024*1024 || usage.MaxRSS != 128*1024*1024 || usage.MaxVMSize != 128*1024*1024 {
		t.Fatalf("unexpected usage: %#v", usage)
	}
}

func TestKubernetesResourceQuantityParsers(t *testing.T) {
	cpuCases := map[string]float64{
		"1":    1,
		"500m": 0.5,
		"250u": 0.00025,
		"10n":  0.00000001,
	}
	for input, want := range cpuCases {
		got, err := parseKubernetesCPU(input)
		if err != nil {
			t.Fatalf("parse CPU %q: %v", input, err)
		}
		if got != want {
			t.Fatalf("parse CPU %q = %v, want %v", input, got, want)
		}
	}

	memoryCases := map[string]int64{
		"1024": 1024,
		"1Ki":  1024,
		"2Mi":  2 * 1024 * 1024,
		"3Gi":  3 * 1024 * 1024 * 1024,
		"4M":   4 * 1000 * 1000,
	}
	for input, want := range memoryCases {
		got, err := parseKubernetesMemoryBytes(input)
		if err != nil {
			t.Fatalf("parse memory %q: %v", input, err)
		}
		if got != want {
			t.Fatalf("parse memory %q = %d, want %d", input, got, want)
		}
	}
}

func TestKubernetesManagerBuildsPVCManifest(t *testing.T) {
	manager, err := NewKubernetesManager(config.KubernetesConfig{
		Namespace:        "judge",
		StorageClassName: "fast",
		WorkdirSize:      "2Gi",
	})
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}

	pvc := manager.buildPVCManifest("Submission_ABC")
	data, err := yaml.Marshal(pvc)
	if err != nil {
		t.Fatalf("marshal pvc: %v", err)
	}
	body := string(data)
	for _, want := range []string{
		"name: csoj-work-submission-abc",
		"namespace: judge",
		"storageClassName: fast",
		"storage: 2Gi",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("pvc manifest missing %q:\n%s", want, body)
		}
	}
}

func TestKubernetesManagerSignalsContainerProcesses(t *testing.T) {
	tmpDir := t.TempDir()
	kubectlPath := filepath.Join(tmpDir, "kubectl")
	logPath := filepath.Join(tmpDir, "kubectl.log")
	t.Setenv("KUBECTL_LOG", logPath)
	if err := os.WriteFile(kubectlPath, []byte("#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$KUBECTL_LOG\"\n"), 0755); err != nil {
		t.Fatalf("write fake kubectl: %v", err)
	}

	manager, err := NewKubernetesManager(config.KubernetesConfig{
		Kubectl:             kubectlPath,
		Namespace:           "judge",
		RunnerContainerName: "runner",
	})
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}

	for _, action := range []struct {
		name string
		run  func() error
		want string
	}{
		{name: "pause", run: func() error { return manager.PauseContainer("pod-1") }, want: "kill -s STOP"},
		{name: "resume", run: func() error { return manager.ResumeContainer("pod-1") }, want: "kill -s CONT"},
		{name: "signal", run: func() error { return manager.SignalContainer("pod-1", "USR1") }, want: "kill -s USR1"},
	} {
		if err := action.run(); err != nil {
			t.Fatalf("%s container: %v", action.name, err)
		}
		body, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("read fake kubectl log: %v", err)
		}
		text := string(body)
		if !strings.Contains(text, "-n judge exec pod-1 -c runner -- /bin/sh -c") || !strings.Contains(text, action.want) {
			t.Fatalf("%s did not issue expected kubectl signal command %q:\n%s", action.name, action.want, text)
		}
	}

	if err := manager.SignalContainer("pod-1", "TERM; touch /tmp/pwned"); err == nil {
		t.Fatalf("unsafe signal should be rejected")
	}
}
