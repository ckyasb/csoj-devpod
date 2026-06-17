package judger

import "github.com/ZJUSCT/CSOJ/internal/database/models"

func ApplyProblemScheduling(sub *models.Submission, problem *Problem) {
	if sub == nil || problem == nil {
		return
	}

	scheduling := problem.Scheduling
	sub.Account = scheduling.Account
	sub.QOS = scheduling.QOS
	sub.Priority = scheduling.Priority
	sub.Nice = scheduling.Nice
	sub.Hold = scheduling.Hold
	if !scheduling.BeginTime.IsZero() {
		beginTime := scheduling.BeginTime
		sub.BeginTime = &beginTime
	}
	if !scheduling.Deadline.IsZero() {
		deadline := scheduling.Deadline
		sub.Deadline = &deadline
	}
	sub.TimeLimit = scheduling.TimeLimit
	sub.Dependencies = scheduling.Dependencies
	sub.Reservation = scheduling.Reservation
	sub.NodeList = scheduling.NodeList
	sub.ExcludeNodes = scheduling.ExcludeNodes
	sub.Constraint = scheduling.Constraint
	sub.GRES = scheduling.GRES
	sub.TRES = scheduling.TRES
}

func CopySubmissionScheduling(dst, src *models.Submission) {
	if dst == nil || src == nil {
		return
	}

	dst.Account = src.Account
	dst.QOS = src.QOS
	dst.Priority = src.Priority
	dst.Nice = src.Nice
	dst.Hold = src.Hold
	dst.JobName = src.JobName
	dst.WorkDir = src.WorkDir
	dst.StdinPath = src.StdinPath
	dst.StdoutPath = src.StdoutPath
	dst.StderrPath = src.StderrPath
	dst.OpenMode = src.OpenMode
	dst.Comment = src.Comment
	dst.MailType = src.MailType
	dst.MailUser = src.MailUser
	dst.Exclusive = src.Exclusive
	dst.Requeue = src.Requeue
	dst.ExportEnv = src.ExportEnv
	dst.Environment = src.Environment
	dst.CPU = src.CPU
	dst.NTasks = src.NTasks
	dst.CPUsPerTask = src.CPUsPerTask
	dst.Nodes = src.Nodes
	dst.Memory = src.Memory
	dst.BeginTime = src.BeginTime
	dst.Deadline = src.Deadline
	dst.TimeLimit = src.TimeLimit
	dst.Dependencies = src.Dependencies
	dst.Reservation = src.Reservation
	dst.NodeList = src.NodeList
	dst.ExcludeNodes = src.ExcludeNodes
	dst.Constraint = src.Constraint
	dst.GRES = src.GRES
	dst.TRES = src.TRES
	dst.Licenses = src.Licenses
	dst.BillingUnits = src.BillingUnits
	dst.Reason = src.Reason
	dst.ArrayJobID = src.ArrayJobID
	dst.ArrayTaskID = src.ArrayTaskID
	dst.ArraySpec = src.ArraySpec
	dst.ArrayTaskCount = src.ArrayTaskCount
	dst.ArrayMaxRunning = src.ArrayMaxRunning
}

func EffectiveCPU(problem *Problem, sub *models.Submission) int {
	if sub != nil && sub.CPU > 0 {
		return sub.CPU
	}
	if problem == nil {
		return 0
	}
	return problem.CPU
}

func EffectiveMemory(problem *Problem, sub *models.Submission) int64 {
	if sub != nil && sub.Memory > 0 {
		return sub.Memory
	}
	if problem == nil {
		return 0
	}
	return problem.Memory
}
