package judger

import (
	"testing"

	"github.com/docker/docker/api/types/container"
)

func TestDockerUsageSamplerAggregatesRuntimeUsage(t *testing.T) {
	sampler := &dockerUsageSampler{
		samples: []dockerUsageSample{
			{cpuTotal: 1_000_000_000, memory: 100, maxRSS: 120, vmSize: 200},
			{cpuTotal: 2_500_000_000, memory: 300, maxRSS: 320, vmSize: 450},
		},
	}

	usage := sampler.Usage()
	if usage.AveCPU != 1.5 || usage.AveRSS != 200 || usage.MaxRSS != 320 || usage.MaxVMSize != 450 {
		t.Fatalf("unexpected usage: %#v", usage)
	}
}

func TestDockerStatsSampleUsesMemoryFallbacks(t *testing.T) {
	sample := dockerStatsSample(container.StatsResponse{
		CPUStats: container.CPUStats{
			CPUUsage: container.CPUUsage{TotalUsage: 42},
		},
		MemoryStats: container.MemoryStats{
			PrivateWorkingSet: 123,
			CommitPeak:        456,
			Commit:            789,
		},
	})

	if sample.cpuTotal != 42 || sample.memory != 123 || sample.maxRSS != 456 || sample.vmSize != 789 {
		t.Fatalf("unexpected stats sample: %#v", sample)
	}
}
