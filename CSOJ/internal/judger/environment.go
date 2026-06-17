package judger

import (
	"fmt"
	"sort"

	"github.com/ZJUSCT/CSOJ/internal/database/models"
)

func SubmissionEnvironmentVariables(sub *models.Submission) []string {
	if sub == nil || len(sub.Environment) == 0 {
		return nil
	}
	names := make([]string, 0, len(sub.Environment))
	for name := range sub.Environment {
		if IsEnvironmentName(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	envs := make([]string, 0, len(names))
	for _, name := range names {
		envs = append(envs, name+"="+fmt.Sprint(sub.Environment[name]))
	}
	return envs
}

func IsEnvironmentName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if i == 0 {
			if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' {
				continue
			}
			return false
		}
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			continue
		}
		return false
	}
	return true
}
