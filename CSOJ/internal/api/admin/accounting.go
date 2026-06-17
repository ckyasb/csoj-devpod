package admin

import (
	"math"
	"net/http"
	"strconv"

	"github.com/ZJUSCT/CSOJ/internal/database"
	"github.com/ZJUSCT/CSOJ/internal/util"
	"github.com/gin-gonic/gin"
)

func (h *Handler) getAccountingRecords(c *gin.Context) {
	pageStr := c.DefaultQuery("page", "1")
	limitStr := c.DefaultQuery("limit", "50")

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	offset := (page - 1) * limit

	filters := map[string]string{
		"submission_id":  c.Query("submission_id"),
		"submission_ids": c.Query("submission_ids"),
		"user_id":        c.Query("user_id"),
		"problem_id":     c.Query("problem_id"),
		"job_name":       c.Query("job_name"),
		"cluster":        c.Query("cluster"),
		"node":           c.Query("node"),
		"account":        c.Query("account"),
		"qos":            c.Query("qos"),
		"array_job_id":   c.Query("array_job_id"),
		"array_task_id":  c.Query("array_task_id"),
		"event":          c.Query("event"),
		"state":          c.Query("state"),
		"start_time":     c.Query("start_time"),
		"end_time":       c.Query("end_time"),
	}

	records, totalItems, err := database.GetAccountingRecords(h.db, filters, limit, offset)
	if err != nil {
		util.Error(c, http.StatusInternalServerError, err)
		return
	}

	util.Success(c, gin.H{
		"items":        records,
		"total_items":  totalItems,
		"total_pages":  int(math.Ceil(float64(totalItems) / float64(limit))),
		"current_page": page,
		"per_page":     limit,
	}, "Accounting records retrieved successfully")
}
