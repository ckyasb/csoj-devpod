package admin

import (
	"github.com/ZJUSCT/CSOJ/internal/config"
	devpodsvc "github.com/ZJUSCT/CSOJ/internal/devpod"
	"github.com/ZJUSCT/CSOJ/internal/judger"
	"gorm.io/gorm"
)

// Handler holds all dependencies for the admin API handlers.
type Handler struct {
	cfg       *config.Config
	db        *gorm.DB
	scheduler *judger.Scheduler
	appState  *judger.AppState
	devpods   *devpodsvc.Manager
}

// NewHandler creates a new admin handler with its dependencies.
func NewHandler(
	cfg *config.Config,
	db *gorm.DB,
	scheduler *judger.Scheduler,
	appState *judger.AppState,
) *Handler {
	return &Handler{
		cfg:       cfg,
		db:        db,
		scheduler: scheduler,
		appState:  appState,
		devpods:   devpodsvc.NewManager(cfg.DevPod),
	}
}
