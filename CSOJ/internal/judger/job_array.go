package judger

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type JobArray struct {
	Spec       string
	TaskIDs    []int
	MaxRunning int
}

func ParseJobArray(spec string) (JobArray, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return JobArray{}, nil
	}

	array := JobArray{Spec: spec}
	rangeSpec := spec
	if before, after, ok := strings.Cut(spec, "%"); ok {
		rangeSpec = before
		maxRunning, err := strconv.Atoi(strings.TrimSpace(after))
		if err != nil || maxRunning <= 0 {
			return JobArray{}, fmt.Errorf("invalid array concurrency limit %q", after)
		}
		array.MaxRunning = maxRunning
	}

	seen := make(map[int]struct{})
	for _, part := range strings.Split(rangeSpec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		taskIDs, err := parseArrayPart(part)
		if err != nil {
			return JobArray{}, err
		}
		for _, taskID := range taskIDs {
			if taskID < 0 {
				return JobArray{}, fmt.Errorf("array task id must be non-negative: %d", taskID)
			}
			if _, ok := seen[taskID]; ok {
				continue
			}
			seen[taskID] = struct{}{}
			array.TaskIDs = append(array.TaskIDs, taskID)
		}
	}
	sort.Ints(array.TaskIDs)
	if len(array.TaskIDs) == 0 {
		return JobArray{}, fmt.Errorf("array spec %q produced no tasks", spec)
	}
	return array, nil
}

func parseArrayPart(part string) ([]int, error) {
	if !strings.Contains(part, "-") {
		value, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid array task id %q", part)
		}
		return []int{value}, nil
	}

	rangePart := part
	step := 1
	if before, after, ok := strings.Cut(part, ":"); ok {
		rangePart = before
		parsedStep, err := strconv.Atoi(after)
		if err != nil || parsedStep <= 0 {
			return nil, fmt.Errorf("invalid array step %q", after)
		}
		step = parsedStep
	}

	startText, endText, ok := strings.Cut(rangePart, "-")
	if !ok {
		return nil, fmt.Errorf("invalid array range %q", part)
	}
	start, err := strconv.Atoi(startText)
	if err != nil {
		return nil, fmt.Errorf("invalid array range start %q", startText)
	}
	end, err := strconv.Atoi(endText)
	if err != nil {
		return nil, fmt.Errorf("invalid array range end %q", endText)
	}
	if end < start {
		return nil, fmt.Errorf("array range end %d is before start %d", end, start)
	}

	taskIDs := make([]int, 0, ((end-start)/step)+1)
	for taskID := start; taskID <= end; taskID += step {
		taskIDs = append(taskIDs, taskID)
	}
	return taskIDs, nil
}
