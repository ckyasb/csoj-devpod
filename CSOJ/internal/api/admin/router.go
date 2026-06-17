package admin

import (
	"github.com/ZJUSCT/CSOJ/internal/api"
	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/embedui"
	"github.com/ZJUSCT/CSOJ/internal/judger"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// NewAdminRouter creates and configures the admin Gin engine.
func NewAdminRouter(
	cfg *config.Config,
	db *gorm.DB,
	scheduler *judger.Scheduler,
	appState *judger.AppState) *gin.Engine {

	r := gin.Default()

	r.Use(api.CORSMiddleware(cfg.CORS))

	h := NewHandler(cfg, db, scheduler, appState)

	v1 := r.Group("/api/v1")
	{
		// Websocket
		v1.GET("/ws/submissions/:id/containers/:conID/logs", h.handleAdminContainerWs)

		// Management
		v1.POST("/reload", h.reload)

		// User Management
		users := v1.Group("/users")
		{
			users.GET("", h.getAllUsers)
			users.POST("", h.createUser)
			users.GET("/:id", h.getUser)
			users.PATCH("/:id", h.updateUser)
			users.DELETE("/:id", h.deleteUser)
			users.GET("/:id/history", h.getUserContestHistory)
			users.POST("/:id/reset-password", h.resetUserPassword)
			users.POST("/:id/register-contest", h.registerUserForContest)
			users.GET("/:id/scores", h.getUserScores)
			users.GET("/:id/download_solutions/:contest_id", h.handleDownloadSolutions)
		}

		// Submission Management
		submissions := v1.Group("/submissions")
		{
			submissions.GET("", h.getAllSubmissions)
			submissions.GET("/:id", h.getSubmission)
			submissions.GET("/:id/content", h.getSubmissionContent)
			submissions.PATCH("/:id", h.updateSubmission)
			submissions.DELETE("/:id", h.deleteSubmission)
			submissions.GET("/:id/containers/:conID/log", h.getContainerLog)
			submissions.POST("/:id/rejudge", h.rejudgeSubmission)
			submissions.POST("/:id/requeue", h.requeueSubmission)
			submissions.PATCH("/:id/validity", h.updateSubmissionValidity)
			submissions.POST("/:id/hold", h.holdSubmission)
			submissions.POST("/:id/release", h.releaseSubmission)
			submissions.POST("/:id/interrupt", h.interruptSubmission)
		}

		// Contest & Problem Management
		contests := v1.Group("/contests")
		{
			contests.GET("", h.getAllContests)
			contests.POST("", h.createContest)
			contests.GET("/:id", h.getContest)
			contests.PUT("/:id", h.updateContest)
			contests.DELETE("/:id", h.deleteContest)
			contests.GET("/:id/leaderboard", h.getContestLeaderboard)
			contests.GET("/:id/trend", h.getContestTrend)
			contests.POST("/:id/problems", h.createProblemInContest)
			contests.PUT("/:id/problems/order", h.handleUpdateContestProblemOrder)
			// Contest Assets
			contests.GET("/:id/assets", h.handleListContestAssets)
			contests.GET("/:id/assets/*assetpath", h.serveContestAsset)
			contests.POST("/:id/assets", h.handleUploadContestAssets)
			contests.DELETE("/:id/assets", h.handleDeleteContestAsset)
			// Contest Announcements
			contests.GET("/:id/announcements", h.handleGetContestAnnouncements)
			contests.POST("/:id/announcements", h.handleCreateContestAnnouncement)
			contests.PUT("/:id/announcements/:announcementId", h.handleUpdateContestAnnouncement)
			contests.DELETE("/:id/announcements/:announcementId", h.handleDeleteContestAnnouncement)
		}

		problems := v1.Group("/problems")
		{
			problems.GET("", h.getAllProblems)
			problems.GET("/:id", h.getProblem)
			problems.PUT("/:id", h.updateProblem)
			problems.DELETE("/:id", h.deleteProblem)
			// Problem Assets
			problems.GET("/:id/assets", h.handleListProblemAssets)
			problems.GET("/:id/assets/*assetpath", h.serveProblemAsset)
			problems.POST("/:id/assets", h.handleUploadProblemAssets)
			problems.DELETE("/:id/assets", h.handleDeleteProblemAsset)
		}

		// Score Management
		scores := v1.Group("/scores")
		{
			scores.POST("/recalculate", h.recalculateScore)
		}

		accounting := v1.Group("/accounting")
		{
			accounting.GET("", h.getAccountingRecords)
		}

		devpods := v1.Group("/devpods")
		{
			devpods.GET("", h.getAllDevPods)
			devpods.GET("/:id", h.getDevPod)
			devpods.DELETE("/:id", h.deleteDevPod)
		}

		slurm := v1.Group("/slurm")
		{
			slurm.POST("/sbatch", h.slurmSubmitBatch)
			slurm.GET("/sinfo", h.slurmSinfo)
			slurm.GET("/squeue", h.slurmSqueue)
			slurm.GET("/sacct", h.slurmSacct)
			slurm.GET("/sreport", h.slurmSreport)
			slurm.GET("/seff", h.slurmSeff)
			slurm.GET("/seff/:id", h.slurmSeff)
			slurm.GET("/sprio", h.slurmSprio)
			slurm.GET("/sshare", h.slurmSshare)
			slurm.GET("/sdiag", h.slurmSdiag)
			slurm.GET("/strigger", h.slurmStriggerList)
			slurm.POST("/strigger", h.slurmStriggerUpsert)
			slurm.POST("/strigger/evaluate", h.slurmStriggerEvaluate)
			slurm.DELETE("/strigger/:id", h.slurmStriggerDelete)
			slurm.DELETE("/strigger", h.slurmStriggerDelete)
			slurm.GET("/scrontab", h.slurmScrontabList)
			slurm.POST("/scrontab", h.slurmScrontabUpsert)
			slurm.PATCH("/scrontab/:id", h.slurmScrontabUpsert)
			slurm.POST("/scrontab/evaluate", h.slurmScrontabEvaluate)
			slurm.DELETE("/scrontab/:id", h.slurmScrontabDelete)
			slurm.DELETE("/scrontab", h.slurmScrontabDelete)
			slurm.GET("/sacctmgr/ping", h.slurmSacctmgrPing)
			slurm.GET("/sacctmgr/show/accounts", h.slurmSacctmgrShowAccounts)
			slurm.GET("/sacctmgr/show/users", h.slurmSacctmgrShowUsers)
			slurm.GET("/sacctmgr/show/user", h.slurmSacctmgrShowUsers)
			slurm.GET("/sacctmgr/show/clusters", h.slurmSacctmgrShowClusters)
			slurm.GET("/sacctmgr/show/cluster", h.slurmSacctmgrShowClusters)
			slurm.GET("/sacctmgr/show/config", h.slurmSacctmgrShowConfig)
			slurm.GET("/sacctmgr/show/stats", h.slurmSacctmgrShowStats)
			slurm.GET("/sacctmgr/show/jobs", h.slurmSacctmgrShowJobs)
			slurm.GET("/sacctmgr/show/job", h.slurmSacctmgrShowJobs)
			slurm.GET("/sacctmgr/show/problems", h.slurmSacctmgrShowProblems)
			slurm.GET("/sacctmgr/show/problem", h.slurmSacctmgrShowProblems)
			slurm.GET("/sacctmgr/show/resources", h.slurmSacctmgrShowResources)
			slurm.GET("/sacctmgr/show/resource", h.slurmSacctmgrShowResources)
			slurm.GET("/sacctmgr/show/runawayjobs", h.slurmSacctmgrShowRunawayJobs)
			slurm.GET("/sacctmgr/show/transactions", h.slurmSacctmgrShowTransactions)
			slurm.GET("/sacctmgr/show/transaction", h.slurmSacctmgrShowTransactions)
			slurm.GET("/sacctmgr/show/events", h.slurmSacctmgrShowEvents)
			slurm.GET("/sacctmgr/show/event", h.slurmSacctmgrShowEvents)
			slurm.GET("/sacctmgr/show/qos", h.slurmSacctmgrShowQOS)
			slurm.GET("/sacctmgr/show/assoc", h.slurmSacctmgrShowAssociations)
			slurm.GET("/sacctmgr/show/tres", h.slurmSacctmgrShowTRES)
			slurm.POST("/sacctmgr/account", h.slurmSacctmgrUpsertAccount)
			slurm.PATCH("/sacctmgr/account/:name", h.slurmSacctmgrUpsertAccount)
			slurm.DELETE("/sacctmgr/account/:name", h.slurmSacctmgrDeleteAccount)
			slurm.POST("/sacctmgr/user", h.slurmSacctmgrUpsertUser)
			slurm.PATCH("/sacctmgr/user/:name", h.slurmSacctmgrUpsertUser)
			slurm.DELETE("/sacctmgr/user/:name", h.slurmSacctmgrDeleteUser)
			slurm.POST("/sacctmgr/qos", h.slurmSacctmgrUpsertQOS)
			slurm.PATCH("/sacctmgr/qos/:name", h.slurmSacctmgrUpsertQOS)
			slurm.DELETE("/sacctmgr/qos/:name", h.slurmSacctmgrDeleteQOS)
			slurm.POST("/sacctmgr/assoc", h.slurmSacctmgrUpsertAssociation)
			slurm.PATCH("/sacctmgr/assoc/:account", h.slurmSacctmgrUpsertAssociation)
			slurm.DELETE("/sacctmgr/assoc/:account", h.slurmSacctmgrDeleteAssociation)
			slurm.GET("/salloc", h.slurmListAllocations)
			slurm.POST("/salloc", h.slurmCreateAllocation)
			slurm.GET("/salloc/:id", h.slurmShowAllocation)
			slurm.POST("/salloc/:id/release", h.slurmReleaseAllocation)
			slurm.DELETE("/salloc/:id", h.slurmReleaseAllocation)
			slurm.POST("/sbcast", h.slurmSbcast)
			slurm.GET("/srun", h.slurmListRunSteps)
			slurm.POST("/srun", h.slurmRun)
			slurm.GET("/srun/:id", h.slurmShowRunStep)
			slurm.GET("/sattach", h.slurmSattach)
			slurm.GET("/sattach/:id", h.slurmSattach)
			slurm.GET("/sstat", h.slurmSstat)
			slurm.GET("/sstat/:id", h.slurmShowStepStat)
			slurm.GET("/scontrol/show/jobs", h.slurmShowJobs)
			slurm.GET("/scontrol/show/job/:id", h.slurmShowJob)
			slurm.GET("/scontrol/show/hostnames", h.slurmShowHostnames)
			slurm.POST("/scontrol/show/hostnames", h.slurmShowHostnames)
			slurm.GET("/scontrol/show/hostlist", h.slurmShowHostlist)
			slurm.POST("/scontrol/show/hostlist", h.slurmShowHostlist)
			slurm.GET("/scontrol/show/steps", h.slurmShowSteps)
			slurm.GET("/scontrol/show/step/:id", h.slurmShowStep)
			slurm.GET("/scontrol/show/nodes", h.slurmShowNodes)
			slurm.GET("/scontrol/show/node/:clusterName/:nodeName", h.slurmShowNode)
			slurm.GET("/scontrol/show/daemons", h.slurmShowDaemons)
			slurm.GET("/scontrol/show/daemon", h.slurmShowDaemons)
			slurm.GET("/scontrol/ping", h.slurmScontrolPing)
			slurm.GET("/scontrol/show/partition", h.slurmShowPartitions)
			slurm.GET("/scontrol/show/config", h.slurmShowConfig)
			slurm.GET("/scontrol/show/licenses", h.slurmShowLicenses)
			slurm.GET("/scontrol/show/reservations", h.slurmShowReservations)
			slurm.POST("/scontrol/update/job/:id", h.slurmUpdateJob)
			slurm.PATCH("/scontrol/update/job/:id", h.slurmUpdateJob)
			slurm.POST("/scontrol/update/node/:clusterName/:nodeName", h.slurmUpdateNode)
			slurm.PATCH("/scontrol/update/node/:clusterName/:nodeName", h.slurmUpdateNode)
			slurm.POST("/scontrol/update/partition/:name", h.slurmUpdatePartition)
			slurm.PATCH("/scontrol/update/partition/:name", h.slurmUpdatePartition)
			slurm.POST("/scontrol/create/reservation", h.slurmUpsertReservation)
			slurm.POST("/scontrol/update/reservation/:name", h.slurmUpsertReservation)
			slurm.PATCH("/scontrol/update/reservation/:name", h.slurmUpsertReservation)
			slurm.DELETE("/scontrol/delete/reservation/:name", h.slurmDeleteReservation)
			slurm.POST("/scontrol/hold/:id", h.slurmHoldJobs)
			slurm.POST("/scontrol/release/:id", h.slurmReleaseJobs)
			slurm.POST("/scontrol/requeue/:id", h.slurmRequeueJobs)
			slurm.POST("/scontrol/suspend/:id", h.slurmSuspendJob)
			slurm.POST("/scontrol/resume/:id", h.slurmResumeJob)
			slurm.POST("/scontrol/signal/:id", h.slurmSignalJob)
			slurm.POST("/scontrol/cancel/:id", h.slurmCancelJobs)
			slurm.POST("/scancel", h.slurmScancel)
			slurm.POST("/scancel/:id/signal", h.slurmSignalJob)
			slurm.POST("/scancel/:id", h.slurmCancelJobs)
		}

		// Cluster Management
		clusters := v1.Group("/clusters")
		{
			clusters.GET("/status", h.getClusterStatus)
			clusters.GET("/queue", h.getSchedulerQueue)
			clusters.GET("/:clusterName/nodes/:nodeName", h.getNodeDetails)
			clusters.POST("/:clusterName/nodes/:nodeName/pause", h.pauseNode)
			clusters.POST("/:clusterName/nodes/:nodeName/resume", h.resumeNode)
			clusters.POST("/:clusterName/nodes/:nodeName/drain", h.drainNode)
			clusters.POST("/:clusterName/nodes/:nodeName/down", h.downNode)
			clusters.POST("/:clusterName/nodes/:nodeName/undrain", h.undrainNode)
		}

		// Container Management
		containers := v1.Group("/containers")
		{
			containers.GET("", h.getAllContainers)
			containers.GET("/:id", h.getContainer)
		}
	}

	embedui.RegisterUIHandlers(r, "admin")

	return r
}
