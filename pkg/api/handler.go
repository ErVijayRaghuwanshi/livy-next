package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"livy-next/pkg/session"
)

// ClientCreator defines the function signature for creating a session's Spark Connect client.
type ClientCreator func(params session.SessionCreateParams) (session.SparkClient, error)

type Handler struct {
	manager            *session.Manager
	createClient       ClientCreator
	sparkUIUrl         string
	sparkHistoryUrl    string
	syncSessionTimeout bool
}

// NewHandler creates a new REST API Handler.
func NewHandler(manager *session.Manager, createClient ClientCreator, sparkUIUrl string, sparkHistoryUrl string, syncSessionTimeout ...bool) *Handler {
	syncTimeout := true
	if len(syncSessionTimeout) > 0 {
		syncTimeout = syncSessionTimeout[0]
	}
	return &Handler{
		manager:            manager,
		createClient:       createClient,
		sparkUIUrl:         sparkUIUrl,
		sparkHistoryUrl:    sparkHistoryUrl,
		syncSessionTimeout: syncTimeout,
	}
}

// CreateSessionRequest specifies the configuration and identity parameters for creating a new session.
type CreateSessionRequest struct {
	// Kind of session: "spark", "pyspark", or "sparkr" (defaults to "spark")
	Kind string `json:"kind" example:"spark"`

	// ProxyUser for legacy Apache Livy compatibility
	ProxyUser string `json:"proxyUser,omitempty" example:"alice"`

	// UserID for Spark Connect multi-tenancy (overrides proxyUser if provided)
	UserID string `json:"userId,omitempty" example:"alice"`

	// SessionID is a client-specified UUID for Spark Connect session isolation and reconnects
	SessionID string `json:"sessionId,omitempty" example:"6002ebfc-3aaf-4d3b-8f98-07b9ae46a51f"`

	// UserAgent identifies the client connecting to Spark Connect
	UserAgent string `json:"userAgent,omitempty" example:"argus-worker"`

	// Token is the authentication token or bearer token forwarded to Spark Connect
	Token string `json:"token,omitempty" example:"secret-token"`

	// Name is an optional human-readable name for the session
	Name string `json:"name,omitempty" example:"etl-session"`

	// Conf contains Spark configuration properties (spark.*)
	Conf map[string]string `json:"conf,omitempty"`

	// Jars is an optional list of JAR paths to attach to the session
	Jars []string `json:"jars,omitempty"`
}

// SessionsResponse represents the response when listing active sessions.
type SessionsResponse struct {
	// From is the result offset for pagination
	From int `json:"from" example:"0"`

	// Total is the total number of active sessions
	Total int `json:"total" example:"1"`

	// Sessions is the list of active sessions
	Sessions []*session.Session `json:"sessions"`

	// IdleTimeout is the server idle timeout threshold in milliseconds
	IdleTimeout int64 `json:"idleTimeout" example:"259200000"`

	// DeadTimeout is the server dead timeout threshold in milliseconds
	DeadTimeout int64 `json:"deadTimeout" example:"86400000"`

	// SparkVersion is the Apache Spark version reported by Spark Connect
	SparkVersion string `json:"sparkVersion,omitempty" example:"4.2.0"`

	// SparkMaster is the Spark Master cluster URL reported by Spark Connect
	SparkMaster string `json:"sparkMaster,omitempty" example:"spark://spark-master:7077"`
}

// CreateStatementRequest specifies a SQL statement to execute with optional tracking tags.
type CreateStatementRequest struct {
	// Code is the SQL statement string to execute
	Code string `json:"code" example:"SELECT 'hello world' AS msg, 42 AS num"`

	// Tags is an optional list of tags to label and track the statement in Spark Connect
	Tags []string `json:"tags,omitempty"`
}

// StatementsResponse represents the response when listing statements within a session.
type StatementsResponse struct {
	// TotalStatements is the total count of statements in the session
	TotalStatements int `json:"total_statements" example:"1"`

	// Statements is the list of submitted statements
	Statements []*session.Statement `json:"statements"`
}

// ErrorResponse represents an error response payload.
type ErrorResponse struct {
	// Error describes the error message
	Error string `json:"error" example:"Session not found"`
}

// DeleteSessionResponse represents the response when a session is deleted.
type DeleteSessionResponse struct {
	// Msg indicates deletion status
	Msg string `json:"msg" example:"deleted"`
}

// CancelStatementResponse represents the response when a statement is cancelled.
type CancelStatementResponse struct {
	// Msg indicates cancellation status
	Msg string `json:"msg" example:"cancelled"`
}

// ListSessions godoc
// @Summary List all active sessions
// @Description Get a list of all active interactive sessions along with server idle and dead timeout settings
// @Tags sessions
// @Accept json
// @Produce json
// @Success 200 {object} SessionsResponse
// @Router /sessions [get]
func (h *Handler) ListSessions(w http.ResponseWriter, r *http.Request) {
	sessions := h.manager.ListSessions()
	resp := SessionsResponse{
		From:         0,
		Total:        len(sessions),
		Sessions:     sessions,
		IdleTimeout:  int64(h.manager.GetIdleTimeout() / time.Millisecond),
		DeadTimeout:  int64(h.manager.GetDeadTimeout() / time.Millisecond),
		SparkVersion: h.manager.GetSparkVersion(),
		SparkMaster:  h.manager.GetSparkMaster(),
	}
	respondJSON(w, http.StatusOK, resp)
}

// CreateSession godoc
// @Summary Create a new session
// @Description Create a new interactive session and connect to Spark Connect with isolated session UUID, multi-tenant user identity, and custom configuration
// @Tags sessions
// @Accept json
// @Produce json
// @Param request body CreateSessionRequest true "Create Session Request"
// @Success 201 {object} session.Session
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 500 {object} ErrorResponse "Failed to connect to Spark Connect"
// @Router /sessions [post]
func (h *Handler) CreateSession(w http.ResponseWriter, r *http.Request) {
	var req CreateSessionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if req.Kind == "" {
		req.Kind = "spark"
	}

	userId := req.UserID
	if userId == "" {
		userId = req.ProxyUser
	}
	userAgent := req.UserAgent
	if userAgent == "" {
		userAgent = "livy-next"
	}

	params := session.SessionCreateParams{
		Name:      req.Name,
		Kind:      req.Kind,
		ProxyUser: req.ProxyUser,
		UserID:    userId,
		SessionID: req.SessionID,
		UserAgent: userAgent,
		Token:     req.Token,
		Conf:      req.Conf,
		Jars:      req.Jars,
	}

	client, err := h.createClient(params)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to connect to Spark Connect server: "+err.Error())
		return
	}

	sess := h.manager.CreateSession(params, client)
	sess.SetState(session.SessionIdle)

	// Fetch Spark application ID from the remote Connect server
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	appId, _ := client.GetAppID(ctx)

	// Populate AppInfo with sparkAppId, live Spark UI link, Spark Connect UI link, and history link
	sess.SetAppInfo(appId, h.sparkUIUrl, h.sparkHistoryUrl)

	// Inherit server-side session timeout from Spark Connect
	if remoteTimeout, err := client.GetSessionTimeout(ctx); err == nil {
		if remoteTimeout > 0 {
			sess.SetIdleTimeout(remoteTimeout)
			if h.syncSessionTimeout {
				h.manager.SetIdleTimeout(remoteTimeout)
			}
		} else if remoteTimeout < 0 {
			// Negative indicates timeout disabled on Spark Connect server
			sess.SetIdleTimeout(-1)
		}
	}

	// Fetch exact Spark runtime version and master info
	if version, err := client.GetSparkVersion(ctx); err == nil && version != "" {
		sess.SparkVersion = version
		master, _ := client.GetMaster(ctx)
		h.manager.SetSparkInfo(version, master)
	}

	respondJSON(w, http.StatusCreated, sess)
}

// VersionResponse represents the version and runtime information of the Livy-Next service and Spark cluster.
type VersionResponse struct {
	// Version is the Livy-Next server version
	Version string `json:"version" example:"1.0.0"`

	// SparkVersion is the version of Apache Spark running on the Connect server
	SparkVersion string `json:"sparkVersion,omitempty" example:"4.2.0"`

	// SparkMaster is the Spark master cluster URL
	SparkMaster string `json:"sparkMaster,omitempty" example:"spark://spark-master:7077"`

	// Build is the build identifier
	Build string `json:"build" example:"livy-next"`
}

// GetVersion godoc
// @Summary Get server and Spark version information
// @Description Get Livy-Next version, Apache Spark Connect version, and cluster details
// @Tags system
// @Accept json
// @Produce json
// @Success 200 {object} VersionResponse
// @Router /version [get]
func (h *Handler) GetVersion(w http.ResponseWriter, r *http.Request) {
	resp := VersionResponse{
		Version:      "1.0.0",
		SparkVersion: h.manager.GetSparkVersion(),
		SparkMaster:  h.manager.GetSparkMaster(),
		Build:        "livy-next",
	}
	respondJSON(w, http.StatusOK, resp)
}

// GetSession godoc
// @Summary Get session details
// @Description Get state, application info, Spark UI, and Spark Connect UI URLs for a specific session by numeric ID or UUID
// @Tags sessions
// @Accept json
// @Produce json
// @Param id path string true "Session ID (integer ID or Spark Connect UUID)" example("0")
// @Success 200 {object} session.Session
// @Failure 404 {object} ErrorResponse "Session not found"
// @Router /sessions/{id} [get]
func (h *Handler) GetSession(w http.ResponseWriter, r *http.Request) {
	sess, exists := h.manager.GetSessionByIdentifier(chi.URLParam(r, "id"))
	if !exists {
		respondError(w, http.StatusNotFound, "Session not found")
		return
	}

	respondJSON(w, http.StatusOK, sess)
}

// DeleteSession godoc
// @Summary Delete session
// @Description Close and terminate a specific session by numeric ID or UUID, releasing Spark Connect resources
// @Tags sessions
// @Accept json
// @Produce json
// @Param id path string true "Session ID (integer ID or Spark Connect UUID)" example("0")
// @Success 200 {object} DeleteSessionResponse "Session deleted message"
// @Failure 404 {object} ErrorResponse "Session not found"
// @Router /sessions/{id} [delete]
func (h *Handler) DeleteSession(w http.ResponseWriter, r *http.Request) {
	if err := h.manager.DeleteSessionByIdentifier(chi.URLParam(r, "id")); err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, DeleteSessionResponse{Msg: "deleted"})
}

// SubmitStatement godoc
// @Summary Submit a statement
// @Description Submit a SQL statement for asynchronous execution within a session with optional tagging
// @Tags statements
// @Accept json
// @Produce json
// @Param id path string true "Session ID (integer ID or Spark Connect UUID)" example("0")
// @Param request body CreateStatementRequest true "Submit Statement Request"
// @Success 201 {object} session.Statement
// @Failure 400 {object} ErrorResponse "Invalid payload or terminated session"
// @Failure 404 {object} ErrorResponse "Session not found"
// @Router /sessions/{id}/statements [post]
func (h *Handler) SubmitStatement(w http.ResponseWriter, r *http.Request) {
	sess, exists := h.manager.GetSessionByIdentifier(chi.URLParam(r, "id"))
	if !exists {
		respondError(w, http.StatusNotFound, "Session not found")
		return
	}

	if sess.GetState() == session.SessionDead || sess.GetState() == session.SessionShuttingDown {
		respondError(w, http.StatusBadRequest, "Cannot submit statement to a terminated session")
		return
	}

	var req CreateStatementRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if req.Code == "" {
		respondError(w, http.StatusBadRequest, "Code cannot be empty")
		return
	}

	stmt := sess.SubmitStatement(req.Code, req.Tags...)
	respondJSON(w, http.StatusCreated, stmt)
}

// ListStatements godoc
// @Summary List statements
// @Description List all statements submitted to a session with pagination support
// @Tags statements
// @Accept json
// @Produce json
// @Param id path string true "Session ID (integer ID or Spark Connect UUID)" example("0")
// @Param from query int false "Offset for pagination" default(0) example(0)
// @Param size query int false "Number of statements to return" example(10)
// @Success 200 {object} StatementsResponse
// @Failure 404 {object} ErrorResponse "Session not found"
// @Router /sessions/{id}/statements [get]
func (h *Handler) ListStatements(w http.ResponseWriter, r *http.Request) {
	sess, exists := h.manager.GetSessionByIdentifier(chi.URLParam(r, "id"))
	if !exists {
		respondError(w, http.StatusNotFound, "Session not found")
		return
	}

	from := 0
	if fStr := r.URL.Query().Get("from"); fStr != "" {
		if f, err := strconv.Atoi(fStr); err == nil && f >= 0 {
			from = f
		}
	}
	size := 0
	if sStr := r.URL.Query().Get("size"); sStr != "" {
		if s, err := strconv.Atoi(sStr); err == nil && s > 0 {
			size = s
		}
	}

	stmts, total := sess.GetStatements(from, size)
	resp := StatementsResponse{
		TotalStatements: total,
		Statements:      stmts,
	}
	respondJSON(w, http.StatusOK, resp)
}

// GetStatement godoc
// @Summary Get statement details
// @Description Get execution state, progress, and result rows of a statement with row pagination
// @Tags statements
// @Accept json
// @Produce json
// @Param id path string true "Session ID (integer ID or Spark Connect UUID)" example("0")
// @Param statementId path int true "Statement ID" example(0)
// @Param from query int false "Result row offset for pagination" default(0) example(0)
// @Param size query int false "Maximum number of rows to return" example(50)
// @Success 200 {object} session.Statement
// @Failure 400 {object} ErrorResponse "Invalid statement ID"
// @Failure 404 {object} ErrorResponse "Session or statement not found"
// @Router /sessions/{id}/statements/{statementId} [get]
func (h *Handler) GetStatement(w http.ResponseWriter, r *http.Request) {
	sess, exists := h.manager.GetSessionByIdentifier(chi.URLParam(r, "id"))
	if !exists {
		respondError(w, http.StatusNotFound, "Session not found")
		return
	}

	stmtID, err := strconv.Atoi(chi.URLParam(r, "statementId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid statement ID")
		return
	}

	from := 0
	if fStr := r.URL.Query().Get("from"); fStr != "" {
		if f, err := strconv.Atoi(fStr); err == nil && f >= 0 {
			from = f
		}
	}
	size := 0
	if sStr := r.URL.Query().Get("size"); sStr != "" {
		if s, err := strconv.Atoi(sStr); err == nil && s > 0 {
			size = s
		}
	}

	stmt, exists := sess.GetStatement(stmtID, from, size)
	if !exists {
		respondError(w, http.StatusNotFound, "Statement not found")
		return
	}

	respondJSON(w, http.StatusOK, stmt)
}

// CancelStatement godoc
// @Summary Cancel a statement
// @Description Cancel a running or waiting statement execution inside a session
// @Tags statements
// @Accept json
// @Produce json
// @Param id path string true "Session ID (integer ID or Spark Connect UUID)" example("0")
// @Param statementId path int true "Statement ID" example(0)
// @Success 200 {object} CancelStatementResponse "Statement cancelled message"
// @Failure 400 {object} ErrorResponse "Invalid statement ID, or statement already completed"
// @Failure 404 {object} ErrorResponse "Session or statement not found"
// @Router /sessions/{id}/statements/{statementId}/cancel [post]
func (h *Handler) CancelStatement(w http.ResponseWriter, r *http.Request) {
	sess, exists := h.manager.GetSessionByIdentifier(chi.URLParam(r, "id"))
	if !exists {
		respondError(w, http.StatusNotFound, "Session not found")
		return
	}

	stmtID, err := strconv.Atoi(chi.URLParam(r, "statementId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid statement ID")
		return
	}

	ok := sess.CancelStatement(stmtID)
	if !ok {
		respondError(w, http.StatusBadRequest, "Statement cannot be cancelled (either not found or already completed)")
		return
	}

	respondJSON(w, http.StatusOK, CancelStatementResponse{Msg: "cancelled"})
}

func respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(response)
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, ErrorResponse{Error: message})
}
