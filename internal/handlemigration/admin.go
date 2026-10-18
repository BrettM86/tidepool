package handlemigration

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

const (
	runStoreTimeout      = 10 * time.Second
	finishMaxAttempts    = 5
	finishInitialBackoff = 100 * time.Millisecond
)

// AdminOptions configures Admin.
type AdminOptions struct {
	Token       string
	Runner      *Runner
	BaseContext context.Context
	Logger      *slog.Logger
	Runs        store.HandleMigrationRuns
}

// Admin serves the handle migration control and progress endpoints.
type Admin struct {
	token       string
	runner      *Runner
	baseContext context.Context
	logger      *slog.Logger
	runStore    store.HandleMigrationRuns
	runMutex    sync.Mutex
	runs        sync.WaitGroup
}

// NewAdmin builds an Admin.
func NewAdmin(opts AdminOptions) (*Admin, error) {
	switch {
	case opts.Token == "":
		return nil, errors.NewValidationError("token", "must not be empty")
	case opts.Runner == nil:
		return nil, errors.NewValidationError("runner", "must not be nil")
	case opts.BaseContext == nil:
		return nil, errors.NewValidationError("base_context", "must not be nil")
	case opts.Runs == nil:
		return nil, errors.NewValidationError("runs", "must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Admin{token: opts.Token, runner: opts.Runner, baseContext: opts.BaseContext, logger: logger, runStore: opts.Runs}, nil
}

// Routes registers the admin routes.
func (a *Admin) Routes(r chi.Router) {
	r.Group(func(r chi.Router) {
		r.Use(a.requireBearer)
		r.Post("/admin/handles/migrate", a.handleMigrate)
		r.Get("/admin/handles/migrate", a.handleProgress)
	})
}

func (a *Admin) requireBearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		header := r.Header.Get("Authorization")
		if len(header) <= len(prefix) || header[:len(prefix)] != prefix ||
			subtle.ConstantTimeCompare([]byte(header[len(prefix):]), []byte(a.token)) != 1 {
			a.logger.Warn("admin request rejected: bad bearer token", "path", r.URL.Path)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *Admin) handleMigrate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DIDs     *[]string       `json:"dids"`
		Rollback json.RawMessage `json:"rollback"`
	}
	// A mistyped or null mode key must not silently start a forward run.
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<16))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil && !stderrors.Is(err, io.EOF) {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	var rollback bool
	if body.Rollback != nil {
		if string(body.Rollback) == "null" || json.Unmarshal(body.Rollback, &rollback) != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
	}
	var extra any
	if err := decoder.Decode(&extra); !stderrors.Is(err, io.EOF) {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	var dids []string
	if body.DIDs != nil {
		if len(*body.DIDs) == 0 {
			http.Error(w, "dids must be non-empty when supplied", http.StatusBadRequest)
			return
		}
		dids = *body.DIDs
	}
	if !a.runMutex.TryLock() {
		http.Error(w, "handle migration already running", http.StatusConflict)
		return
	}
	// A client disconnect after the INSERT commits must not orphan the row.
	startContext, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), runStoreTimeout)
	mode := store.HandleMigrationModeForward
	if rollback {
		mode = store.HandleMigrationModeRollback
	}
	run, err := a.runStore.Start(startContext, mode, dids)
	cancel()
	if err != nil {
		a.runMutex.Unlock()
		if errors.IsAlreadyExists(err) {
			http.Error(w, "handle migration already running", http.StatusConflict)
		} else {
			a.logger.Error("start handle migration failed", "error", err)
			http.Error(w, "could not start handle migration", http.StatusInternalServerError)
		}
		return
	}
	a.startRun(run)
	w.WriteHeader(http.StatusAccepted)
}

// startRun takes ownership of runMutex, held by POST or boot resume.
func (a *Admin) startRun(run *store.HandleMigrationRun) {
	a.runs.Add(1)
	go func() {
		defer a.runs.Done()
		defer a.runMutex.Unlock()
		var err error
		switch run.Mode {
		case store.HandleMigrationModeForward:
			err = a.runner.Run(a.baseContext, run.DIDs)
		case store.HandleMigrationModeRollback:
			err = a.runner.Rollback(a.baseContext, run.DIDs)
		default:
			err = fmt.Errorf("unsupported handle migration mode %q", run.Mode)
		}
		if err != nil {
			a.logger.Error("handle migration stopped", "error", err)
		}
		// Shutdown leaves the run active so the next boot resumes it.
		if a.baseContext.Err() == nil {
			a.finishRun(run.ID)
		}
	}()
}

// finishRun retries Finish with backoff while the base context is live, so a
// database blip does not leave an active row that nothing runs.
func (a *Admin) finishRun(id int64) {
	backoff := finishInitialBackoff
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(a.baseContext), runStoreTimeout)
		err := a.runStore.Finish(ctx, id)
		cancel()
		if err == nil {
			return
		}
		// An earlier attempt may have committed before reporting an error.
		if errors.IsNotFound(err) {
			a.logger.Warn("handle migration run already finished", "run_id", id, "attempt", attempt)
			return
		}
		a.logger.Error("finish handle migration run failed", "run_id", id, "attempt", attempt, "error", err)
		if attempt == finishMaxAttempts {
			a.logger.Error("handle migration run left active: finish retries exhausted", "run_id", id)
			return
		}
		timer := time.NewTimer(backoff)
		select {
		case <-a.baseContext.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff *= 2
	}
}

func (a *Admin) handleProgress(w http.ResponseWriter, r *http.Request) {
	run, err := a.runStore.Active(r.Context())
	if err != nil {
		a.logger.Error("read active handle migration run failed", "error", err)
		http.Error(w, "could not read handle migration progress", http.StatusInternalServerError)
		return
	}
	progress, err := a.runStore.Progress(r.Context(), a.runner.bridgeHostname, a.runner.handleZoneRoot)
	if err != nil {
		a.logger.Error("count handle migration progress failed", "error", err)
		http.Error(w, "could not read handle migration progress", http.StatusInternalServerError)
		return
	}
	response := struct {
		Running           bool           `json:"running"`
		Mode              *string        `json:"mode"`
		StartedAt         *string        `json:"started_at"`
		DIDLimited        bool           `json:"did_limited"`
		Remaining         int            `json:"remaining"`
		RollbackRemaining int            `json:"rollback_remaining"`
		Pending           int            `json:"pending"`
		Migrated          int            `json:"migrated"`
		Errors            map[string]int `json:"errors"`
	}{Running: run != nil, Remaining: progress.Remaining, RollbackRemaining: progress.RollbackRemaining,
		Pending: progress.Pending, Migrated: progress.Migrated, Errors: progress.Errors}
	if run != nil {
		mode := string(run.Mode)
		response.Mode = &mode
		startedAt := run.StartedAt.UTC().Format(time.RFC3339)
		response.StartedAt = &startedAt
		response.DIDLimited = run.DIDs != nil
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		a.logger.Error("write handle migration progress failed", "error", err)
	}
}

// Wait blocks until a background run ends.
func (a *Admin) Wait() { a.runs.Wait() }

// ResumeActiveRun restarts the unfinished persisted run at boot.
func (a *Admin) ResumeActiveRun(ctx context.Context) error {
	if !a.runMutex.TryLock() {
		return errors.NewValidationError("run", "handle migration already running")
	}
	run, err := a.runStore.Active(ctx)
	if err != nil {
		a.runMutex.Unlock()
		return fmt.Errorf("resume handle migration: %w", err)
	}
	if run == nil {
		a.runMutex.Unlock()
		return nil
	}
	a.startRun(run)
	return nil
}
