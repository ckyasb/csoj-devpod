package admin

import (
	"fmt"
	"net/http"

	"github.com/ZJUSCT/CSOJ/internal/util"
	"github.com/gin-gonic/gin"
)

func (h *Handler) getClusterStatus(c *gin.Context) {
	// This structure combines resource status and queue status
	type ClusterStatusResponse struct {
		ResourceStatus interface{}    `json:"resource_status"`
		QueueLengths   map[string]int `json:"queue_lengths"`
		LicenseStatus  interface{}    `json:"license_status"`
	}

	status := h.scheduler.GetClusterStates()
	queueLengths := h.scheduler.GetQueueLengths()
	licenseStatus := h.scheduler.GetLicenseStatus()

	response := ClusterStatusResponse{
		ResourceStatus: status,
		QueueLengths:   queueLengths,
		LicenseStatus:  licenseStatus,
	}

	util.Success(c, response, "Cluster status retrieved")
}

func (h *Handler) getNodeDetails(c *gin.Context) {
	clusterName := c.Param("clusterName")
	nodeName := c.Param("nodeName")

	details, err := h.scheduler.GetNodeDetails(clusterName, nodeName)
	if err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	util.Success(c, details, "Node details retrieved successfully")
}

func (h *Handler) getSchedulerQueue(c *gin.Context) {
	entries, err := h.scheduler.GetQueueSnapshot()
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}
	util.Success(c, entries, "Scheduler queue retrieved")
}

func (h *Handler) pauseNode(c *gin.Context) {
	clusterName := c.Param("clusterName")
	nodeName := c.Param("nodeName")

	if err := h.scheduler.PauseNode(clusterName, nodeName); err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	util.Success(c, nil, fmt.Sprintf("Node '%s/%s' paused successfully", clusterName, nodeName))
}

func (h *Handler) resumeNode(c *gin.Context) {
	clusterName := c.Param("clusterName")
	nodeName := c.Param("nodeName")

	if err := h.scheduler.ResumeNode(clusterName, nodeName); err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	util.Success(c, nil, fmt.Sprintf("Node '%s/%s' resumed successfully", clusterName, nodeName))
}

func (h *Handler) drainNode(c *gin.Context) {
	h.setNodeState(c, "drain")
}

func (h *Handler) downNode(c *gin.Context) {
	h.setNodeState(c, "down")
}

func (h *Handler) undrainNode(c *gin.Context) {
	h.setNodeState(c, "idle")
}

func (h *Handler) setNodeState(c *gin.Context, state string) {
	clusterName := c.Param("clusterName")
	nodeName := c.Param("nodeName")

	var req struct {
		Reason string `json:"reason"`
	}
	_ = c.ShouldBindJSON(&req)

	if err := h.scheduler.SetNodeState(clusterName, nodeName, state, req.Reason); err != nil {
		util.Error(c, http.StatusNotFound, err)
		return
	}
	util.Success(c, nil, fmt.Sprintf("Node '%s/%s' state set to '%s'", clusterName, nodeName, state))
}
