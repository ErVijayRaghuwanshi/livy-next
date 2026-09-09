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
	manager         *session.Manager
	createClient    ClientCreator
	sparkUIUrl      string
	sparkHistoryUrl string
}

// NewHandler creates a new REST API Handler.
func NewHandler(manager *session.Manager, createClient ClientCreator, sparkUIUrl string, sparkHistoryUrl string) *Handler {
	return &Handler{
		manager:         manager,
		createClient:    createClient,
		sparkUIUrl:      sparkUIUrl,
		sparkHistoryUrl: sparkHistoryUrl,
	}
}

type CreateSessionRequest struct {
	Kind      string            `json:"kind"`
	ProxyUser string            `json:"proxyUser"`
	UserID    string            `json:"userId"`
	SessionID string            `json:"sessionId"`
	UserAgent string            `json:"userAgent"`
	Token     string            `json:"token"`
	Name      string            `json:"name"`
	Conf      map[string]string `json:"conf"`
	Jars      []string          `json:"jars"`
}

type SessionsResponse struct {
	From        int                `json:"from"`
	Total       int                `json:"total"`
	Sessions    []*session.Session `json:"sessions"`
	IdleTimeout int64              `json:"idleTimeout"` // in milliseconds
	DeadTimeout int64              `json:"deadTimeout"` // in milliseconds
}

type CreateStatementRequest struct {
	Code string   `json:"code"`
	Tags []string `json:"tags,omitempty"`
}

type StatementsResponse struct {
	TotalStatements int                  `json:"total_statements"`
	Statements      []*session.Statement `json:"statements"`
}

// ListSessions godoc
// @Summary List all active sessions
// @Description Get a list of all active interactive sessions
// @Tags sessions
// @Accept json
// @Produce json
// @Success 200 {object} SessionsResponse
// @Router /sessions [get]
func (h *Handler) ListSessions(w http.ResponseWriter, r *http.Request) {
	sessions := h.manager.ListSessions()
	resp := SessionsResponse{
		From:        0,
		Total:       len(sessions),
		Sessions:    sessions,
		IdleTimeout: int64(h.manager.GetIdleTimeout() / time.Millisecond),
		DeadTimeout: int64(h.manager.GetDeadTimeout() / time.Millisecond),
	}
	respondJSON(w, http.StatusOK, resp)
}

// CreateSession godoc
// @Summary Create a new session
// @Description Create a new interactive session and connect to Spark Connect
// @Tags sessions
// @Accept json
// @Produce json
// @Param request body CreateSessionRequest true "Create Session Request"
// @Success 201 {object} session.Session
// @Failure 400 {object} map[string]string "Invalid request payload"
// @Failure 500 {object} map[string]string "Failed to connect to Spark Connect"
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

	respondJSON(w, http.StatusCreated, sess)
}

// GetSession godoc
// @Summary Get session details
// @Description Get details and state of a specific session
// @Tags sessions
// @Accept json
// @Produce json
// @Param id path int true "Session ID"
// @Success 200 {object} session.Session
// @Failure 400 {object} map[string]string "Invalid session ID"
// @Failure 404 {object} map[string]string "Session not found"
// @Router /sessions/{id} [get]
func (h *Handler) GetSession(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid session ID")
		return
	}

	sess, exists := h.manager.GetSession(id)
	if !exists {
		respondError(w, http.StatusNotFound, "Session not found")
		return
	}

	respondJSON(w, http.StatusOK, sess)
}

// DeleteSession godoc
// @Summary Delete session
// @Description Close and terminate a specific session
// @Tags sessions
// @Accept json
// @Produce json
// @Param id path int true "Session ID"
// @Success 200 {object} map[string]string "Session deleted msg"
// @Failure 400 {object} map[string]string "Invalid session ID"
// @Failure 404 {object} map[string]string "Session not found"
// @Router /sessions/{id} [delete]
func (h *Handler) DeleteSession(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid session ID")
		return
	}

	if err := h.manager.DeleteSession(id); err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{"msg": "deleted"})
}

// SubmitStatement godoc
// @Summary Submit a statement
// @Description Submit a SQL statement for execution in a session
// @Tags statements
// @Accept json
// @Produce json
// @Param id path int true "Session ID"
// @Param request body CreateStatementRequest true "Submit Statement Request"
// @Success 201 {object} session.Statement
// @Failure 400 {object} map[string]string "Invalid payload/ID"
// @Failure 404 {object} map[string]string "Session not found"
// @Router /sessions/{id}/statements [post]
func (h *Handler) SubmitStatement(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid session ID")
		return
	}

	sess, exists := h.manager.GetSession(id)
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
// @Description List all statements submitted to a session
// @Tags statements
// @Accept json
// @Produce json
// @Param id path int true "Session ID"
// @Param from query int false "Offset for pagination"
// @Param size query int false "Number of statements to return"
// @Success 200 {object} StatementsResponse
// @Failure 400 {object} map[string]string "Invalid session ID"
// @Failure 404 {object} map[string]string "Session not found"
// @Router /sessions/{id}/statements [get]
func (h *Handler) ListStatements(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid session ID")
		return
	}

	sess, exists := h.manager.GetSession(id)
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
// @Description Get execution state and results of a statement with optional row pagination
// @Tags statements
// @Accept json
// @Produce json
// @Param id path int true "Session ID"
// @Param statementId path int true "Statement ID"
// @Param from query int false "Result row offset"
// @Param size query int false "Maximum number of rows to return"
// @Success 200 {object} session.Statement
// @Failure 400 {object} map[string]string "Invalid session/statement ID"
// @Failure 404 {object} map[string]string "Session/Statement not found"
// @Router /sessions/{id}/statements/{statementId} [get]
func (h *Handler) GetStatement(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid session ID")
		return
	}

	sess, exists := h.manager.GetSession(id)
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
// @Description Cancel a statement execution inside a session
// @Tags statements
// @Accept json
// @Produce json
// @Param id path int true "Session ID"
// @Param statementId path int true "Statement ID"
// @Success 200 {object} map[string]string "msg: cancelled"
// @Failure 400 {object} map[string]string "Invalid session/statement ID"
// @Failure 404 {object} map[string]string "Session/Statement not found"
// @Router /sessions/{id}/statements/{statementId}/cancel [post]
func (h *Handler) CancelStatement(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid session ID")
		return
	}

	sess, exists := h.manager.GetSession(id)
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

	respondJSON(w, http.StatusOK, map[string]string{"msg": "cancelled"})
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
	respondJSON(w, status, map[string]string{"error": message})
}
