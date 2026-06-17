package judger

import (
	"sort"
	"strings"
)

type LicenseStatus struct {
	Name      string         `json:"name"`
	Total     int            `json:"total"`
	Used      int            `json:"used"`
	Available int            `json:"available"`
	Owners    map[string]int `json:"owners"`
}

func (s *Scheduler) initLicenses(configured map[string]int) {
	s.licenseTotals = make(map[string]int)
	s.licenseUsed = make(map[string]int)
	s.licenseOwners = make(map[string]map[string]int)
	for name, total := range configured {
		normalized := normalizeLicenseName(name)
		if normalized == "" || total <= 0 {
			continue
		}
		s.licenseTotals[normalized] += total
	}
}

func (s *Scheduler) GetLicenseStatus() []LicenseStatus {
	s.licenseMu.Lock()
	defer s.licenseMu.Unlock()

	names := make([]string, 0, len(s.licenseTotals))
	for name := range s.licenseTotals {
		names = append(names, name)
	}
	sort.Strings(names)

	statuses := make([]LicenseStatus, 0, len(names))
	for _, name := range names {
		total := s.licenseTotals[name]
		used := s.licenseUsed[name]
		owners := make(map[string]int, len(s.licenseOwners[name]))
		for owner, count := range s.licenseOwners[name] {
			owners[owner] = count
		}
		statuses = append(statuses, LicenseStatus{
			Name:      name,
			Total:     total,
			Used:      used,
			Available: total - used,
			Owners:    owners,
		})
	}
	return statuses
}

func (s *Scheduler) AcquireLicensesForJob(job QueuedSubmission, owner string) bool {
	requested := requestedLicenses(job)
	if len(requested) == 0 || owner == "" {
		return true
	}

	s.licenseMu.Lock()
	defer s.licenseMu.Unlock()

	for name, count := range requested {
		if s.licenseTotals[name] <= 0 || s.licenseTotals[name]-s.licenseUsed[name] < count {
			return false
		}
	}
	for name, count := range requested {
		s.licenseUsed[name] += count
		if s.licenseOwners[name] == nil {
			s.licenseOwners[name] = make(map[string]int)
		}
		s.licenseOwners[name][owner] += count
	}
	return true
}

func (s *Scheduler) ReleaseLicenses(owner string) {
	if owner == "" {
		return
	}

	s.licenseMu.Lock()
	defer s.licenseMu.Unlock()

	for name, owners := range s.licenseOwners {
		count := owners[owner]
		if count <= 0 {
			continue
		}
		s.licenseUsed[name] -= count
		if s.licenseUsed[name] < 0 {
			s.licenseUsed[name] = 0
		}
		delete(owners, owner)
	}
}

func requestedLicenses(job QueuedSubmission) map[string]int {
	spec := ""
	if job.Problem != nil {
		spec = job.Problem.Scheduling.TRES
	}
	if job.Submission != nil && job.Submission.TRES != "" {
		spec = job.Submission.TRES
	}

	out := make(map[string]int)
	for _, item := range splitList(spec) {
		name, count, err := parseGRES(item)
		if err != nil {
			continue
		}
		normalized := normalizeLicenseName(name)
		if normalized == "" {
			continue
		}
		out[normalized] += count
	}
	return out
}

func normalizeLicenseName(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	switch {
	case strings.HasPrefix(name, "license/"):
		return name
	case strings.HasPrefix(name, "licenses/"):
		return "license/" + strings.TrimPrefix(name, "licenses/")
	case strings.HasPrefix(name, "license:"):
		return "license/" + strings.TrimPrefix(name, "license:")
	default:
		return ""
	}
}
