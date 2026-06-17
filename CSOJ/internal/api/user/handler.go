package user

import (
	"github.com/ZJUSCT/CSOJ/internal/auth"
	"github.com/ZJUSCT/CSOJ/internal/config"
	devpodsvc "github.com/ZJUSCT/CSOJ/internal/devpod"
	"github.com/ZJUSCT/CSOJ/internal/judger"
	"gorm.io/gorm"
)

// Handler holds all dependencies for the user API handlers.
type Handler struct {
	cfg               *config.Config
	db                *gorm.DB
	scheduler         *judger.Scheduler
	appState          *judger.AppState
	devpodManager     *devpodsvc.Manager
	gitlabAuthHandler *auth.GitLabHandler
}

// NewHandler creates a new user handler with its dependencies.
func NewHandler(
	cfg *config.Config,
	db *gorm.DB,
	scheduler *judger.Scheduler,
	appState *judger.AppState,
) *Handler {
	return &Handler{
		cfg:               cfg,
		db:                db,
		scheduler:         scheduler,
		appState:          appState,
		devpodManager:     devpodsvc.NewManager(cfg.DevPod),
		gitlabAuthHandler: auth.NewGitLabHandler(cfg, db),
	}
}
