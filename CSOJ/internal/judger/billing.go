package judger

import (
	"strings"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
)

func CalculateBilling(cfg *config.Config, problem *Problem, sub *models.Submission) float64 {
	if problem == nil {
		return 0
	}
	weights := map[string]float64{}
	if cfg != nil && cfg.Scheduler.BillingWeights != nil {
		weights = cfg.Scheduler.BillingWeights
	}

	total := 0.0
	total += float64(schedulingCPUForSubmission(problem, sub)) * billingWeight(weights, "cpu")
	total += float64(EffectiveMemory(problem, sub)*int64(billingNodeCount(sub))) * billingWeight(weights, "mem", "memory", "mem_mb")

	gresSpec := problem.Scheduling.GRES
	tresSpec := problem.Scheduling.TRES
	if sub != nil {
		if sub.GRES != "" {
			gresSpec = sub.GRES
		}
		if sub.TRES != "" {
			tresSpec = sub.TRES
		}
	}
	total += billingForResourceList(weights, gresSpec)
	total += billingForResourceList(weights, tresSpec)
	return total
}

func billingNodeCount(sub *models.Submission) int {
	if sub != nil && sub.Nodes > 0 {
		return sub.Nodes
	}
	return 1
}

func EffectiveBilling(cfg *config.Config, problem *Problem, sub *models.Submission) float64 {
	if sub != nil && sub.BillingUnits > 0 {
		return sub.BillingUnits
	}
	return CalculateBilling(cfg, problem, sub)
}

func billingForResourceList(weights map[string]float64, spec string) float64 {
	total := 0.0
	for _, item := range splitList(spec) {
		name, count, err := parseGRES(item)
		if err != nil {
			continue
		}
		total += float64(count) * billingWeight(weights, name, normalizeTRESName(name))
	}
	return total
}

func billingWeight(weights map[string]float64, keys ...string) float64 {
	for _, key := range keys {
		if weight, ok := weights[key]; ok {
			return weight
		}
	}
	return 0
}

func normalizeTRESName(name string) string {
	if strings.HasPrefix(name, "gres/") {
		return strings.TrimPrefix(name, "gres/")
	}
	return name
}
