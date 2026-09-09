package session

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// SessionState represents the current state of a Livy session.
type SessionState string

const (
	SessionStarting     SessionState = "starting"
	SessionIdle         SessionState = "idle"
	SessionBusy         SessionState = "busy"
	SessionDead         SessionState = "dead"
	SessionShuttingDown SessionState = "shutting_down"
)

// StatementState represents the current execution state of a statement.
type StatementState string

const (
	StatementWaiting    StatementState = "waiting"
	StatementRunning    StatementState = "running"
	StatementAvailable  StatementState = "available"
	StatementError      StatementState = "error"
	StatementCancelling StatementState = "cancelling"
	StatementCancelled  StatementState = "cancelled"
)

// SparkClient interface abstracts the Spark Connect execution logic.
type SparkClient interface {
	ExecuteSQL(ctx context.Context, sql string) (*QueryResult, error)
	GetAppID(ctx context.Context) (string, error)
	GetSessionID() string
	SetConfig(ctx context.Context, key string, value string) error
	Close() error
}

// SessionCreateParams specifies configuration and identity parameters for creating a new session.
type SessionCreateParams struct {
	Name          string            `json:"name"`
	Kind          string            `json:"kind"`
	ProxyUser     string            `json:"proxyUser"`
	UserID        string            `json:"userId"`
	SessionID     string            `json:"sessionId"`
	UserAgent     string            `json:"userAgent"`
	Token         string            `json:"token"`
	Conf          map[string]string `json:"conf"`
	Jars          []string          `json:"jars"`
	MaxResultRows int               `json:"maxResultRows"`
}

// QueryResult represents the parsed results of a Spark SQL statement with optional pagination.
type QueryResult struct {
	Schema interface{}     `json:"schema"`
	Data   [][]interface{} `json:"data"`
	Total  int             `json:"total,omitempty"`
	From   int             `json:"from,omitempty"`
	Size   int             `json:"size,omitempty"`
}

// SchemaField represents a single field inside a Spark Schema struct.
type SchemaField struct {
	Name     string                 `json:"name"`
	Type     string                 `json:"type"`
	Nullable bool                   `json:"nullable"`
	Metadata map[string]interface{} `json:"metadata"`
}

// Schema represents a Spark SQL StructType schema.
type Schema struct {
	Type   string        `json:"type"`
	Fields []SchemaField `json:"fields"`
}

// StatementOutput represents the final output of a statement.
type StatementOutput struct {
	Status         string                 `json:"status"` // "ok" or "error"
	ExecutionCount int                    `json:"execution_count"`
	Data           map[string]interface{} `json:"data"` // e.g. "application/json" -> QueryResult or "text/plain" -> string
	Ename          string                 `json:"ename,omitempty"`
	Evalue         string                 `json:"evalue,omitempty"`
	Traceback      []string               `json:"traceback,omitempty"`
}

// Statement represents a code statement executed within a Session.
type Statement struct {
	ID        int              `json:"id"`
	Code      string           `json:"code"`
	State     StatementState   `json:"state"`
	Output    *StatementOutput `json:"output,omitempty"`
	Progress  float64          `json:"progress"`
	Started   int64            `json:"started,omitempty"`   // Milliseconds epoch
	Completed int64            `json:"completed,omitempty"` // Milliseconds epoch
	Tags      []string         `json:"tags,omitempty"`

	cancelFunc context.CancelFunc `json:"-"`
}

// Session represents an interactive Livy session.
type Session struct {
	ID           int               `json:"id"`
	Name         string            `json:"name,omitempty"`
	AppID        string            `json:"appId,omitempty"`
	SessionID    string            `json:"sessionId,omitempty"`
	UserID       string            `json:"userId,omitempty"`
	UserAgent    string            `json:"userAgent,omitempty"`
	Owner        string            `json:"owner,omitempty"`
	ProxyUser    string            `json:"proxyUser,omitempty"`
	State        SessionState      `json:"state"`
	Kind         string            `json:"kind"` // "spark", "pyspark", "sparkr"
	AppInfo      map[string]string `json:"appInfo"`
	Log          []string          `json:"log"`
	Statements   []*Statement      `json:"-"`
	LastActivity time.Time         `json:"lastActivity"`

	client  SparkClient
	mu      sync.RWMutex
	closeCh chan struct{}
}

// NewSession creates and initializes a new Session.
func NewSession(id int, params SessionCreateParams, client SparkClient) *Session {
	sessionId := params.SessionID
	if sessionId == "" && client != nil {
		sessionId = client.GetSessionID()
	}
	userId := params.UserID
	if userId == "" {
		userId = params.ProxyUser
	}
	userAgent := params.UserAgent
	if userAgent == "" {
		userAgent = "livy-next"
	}
	kind := params.Kind
	if kind == "" {
		kind = "spark"
	}
	return &Session{
		ID:           id,
		Name:         params.Name,
		State:        SessionStarting,
		Kind:         kind,
		SessionID:    sessionId,
		UserID:       userId,
		UserAgent:    userAgent,
		ProxyUser:    params.ProxyUser,
		AppInfo:      make(map[string]string),
		Log:          []string{"Session created"},
		Statements:   make([]*Statement, 0),
		LastActivity: time.Now(),
		client:       client,
		closeCh:      make(chan struct{}),
	}
}

// SetState updates the session state and updates LastActivity time.
func (s *Session) SetState(state SessionState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.State = state
	s.LastActivity = time.Now()
}

// GetState returns the current state.
func (s *Session) GetState() SessionState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.State
}

// AddLog appends a message to the session log.
func (s *Session) AddLog(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Log = append(s.Log, msg)
}

// GetLogs returns session log lines.
func (s *Session) GetLogs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Log
}

// Close closes the session's Spark Connect connection.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.State == SessionDead || s.State == SessionShuttingDown {
		return nil
	}
	s.State = SessionShuttingDown
	s.Log = append(s.Log, "Session shutting down")
	close(s.closeCh)
	var err error
	if s.client != nil {
		err = s.client.Close()
	}
	s.State = SessionDead
	s.Log = append(s.Log, "Session terminated")
	s.LastActivity = time.Now()
	return err
}

// SubmitStatement submits a SQL query/statement to be executed in the session.
func (s *Session) SubmitStatement(code string, tags ...string) *Statement {
	s.mu.Lock()
	defer s.mu.Unlock()

	stmtID := len(s.Statements)
	stmt := &Statement{
		ID:       stmtID,
		Code:     code,
		State:    StatementWaiting,
		Progress: 0.0,
		Started:  time.Now().UnixNano() / int64(time.Millisecond),
		Tags:     tags,
	}
	s.Statements = append(s.Statements, stmt)
	s.State = SessionBusy
	s.LastActivity = time.Now()

	// Execute asynchronously
	go s.runStatement(stmt)

	return stmt
}

func (s *Session) runStatement(stmt *Statement) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.mu.Lock()
	if stmt.State == StatementCancelled {
		s.mu.Unlock()
		return
	}
	stmt.cancelFunc = cancel
	stmt.State = StatementRunning
	stmt.Progress = 0.1
	s.mu.Unlock()

	// Listen for session close
	go func() {
		select {
		case <-s.closeCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	res, err := s.client.ExecuteSQL(ctx, stmt.Code)

	s.mu.Lock()
	defer s.mu.Unlock()

	stmt.Completed = time.Now().UnixNano() / int64(time.Millisecond)
	stmt.Progress = 1.0

	// If cancelled by user while executing, keep cancelled state
	if stmt.State == StatementCancelled {
		s.updateBusyStateLocked()
		return
	}

	if err != nil {
		stmt.State = StatementError
		stmt.Output = &StatementOutput{
			Status:         "error",
			ExecutionCount: stmt.ID,
			Ename:          "SparkExecutionError",
			Evalue:         err.Error(),
			Traceback:      []string{err.Error()},
		}
		s.Log = append(s.Log, "Statement execution failed: "+err.Error())

		// If the Spark session was closed on the server side, automatically terminate this session locally
		if strings.Contains(err.Error(), "SESSION_CLOSED") || strings.Contains(err.Error(), "INVALID_HANDLE") {
			go s.Close()
		}
	} else {
		stmt.State = StatementAvailable
		stmt.Output = &StatementOutput{
			Status:         "ok",
			ExecutionCount: stmt.ID,
			Data: map[string]interface{}{
				"application/json": res,
			},
		}
		s.Log = append(s.Log, "Statement executed successfully")
	}

	s.updateBusyStateLocked()
	s.LastActivity = time.Now()
}

func (s *Session) updateBusyStateLocked() {
	allDone := true
	for _, st := range s.Statements {
		if st.State == StatementWaiting || st.State == StatementRunning {
			allDone = false
			break
		}
	}
	if allDone && s.State == SessionBusy {
		s.State = SessionIdle
	}
}

// GetStatement retrieves a specific statement by ID with optional pagination (from, size).
// Results are preserved in session memory for subsequent reads and pagination.
func (s *Session) GetStatement(id int, from int, size int) (*Statement, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if id < 0 || id >= len(s.Statements) {
		return nil, false
	}
	stmt := s.Statements[id]

	stmtCopy := &Statement{
		ID:        stmt.ID,
		Code:      stmt.Code,
		State:     stmt.State,
		Progress:  stmt.Progress,
		Started:   stmt.Started,
		Completed: stmt.Completed,
		Tags:      stmt.Tags,
	}

	if stmt.Output != nil {
		stmtCopy.Output = paginateOutput(stmt.Output, from, size)
	}

	return stmtCopy, true
}

func paginateOutput(out *StatementOutput, from int, size int) *StatementOutput {
	if out == nil {
		return nil
	}
	copyOut := &StatementOutput{
		Status:         out.Status,
		ExecutionCount: out.ExecutionCount,
		Ename:          out.Ename,
		Evalue:         out.Evalue,
		Traceback:      out.Traceback,
		Data:           make(map[string]interface{}),
	}
	for k, v := range out.Data {
		copyOut.Data[k] = v
	}

	if from <= 0 && size <= 0 {
		return copyOut
	}

	rawJSON, exists := copyOut.Data["application/json"]
	if !exists || rawJSON == nil {
		return copyOut
	}

	var qr *QueryResult
	switch val := rawJSON.(type) {
	case *QueryResult:
		qr = val
	case QueryResult:
		qr = &val
	}

	if qr == nil {
		return copyOut
	}

	total := len(qr.Data)
	start := from
	if start < 0 {
		start = 0
	}
	if start > total {
		start = total
	}

	end := total
	if size > 0 {
		end = start + size
		if end > total {
			end = total
		}
	}

	slicedData := qr.Data[start:end]
	copyOut.Data["application/json"] = &QueryResult{
		Schema: qr.Schema,
		Data:   slicedData,
		Total:  total,
		From:   start,
		Size:   len(slicedData),
	}

	return copyOut
}

// GetStatements retrieves statements with optional pagination (from, size).
func (s *Session) GetStatements(from int, size int) ([]*Statement, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	total := len(s.Statements)
	if total == 0 {
		return []*Statement{}, 0
	}

	start := from
	if start < 0 {
		start = 0
	}
	if start > total {
		start = total
	}

	end := total
	if size > 0 {
		end = start + size
		if end > total {
			end = total
		}
	}

	stmts := s.Statements[start:end]
	res := make([]*Statement, len(stmts))
	for i, st := range stmts {
		res[i] = &Statement{
			ID:        st.ID,
			Code:      st.Code,
			State:     st.State,
			Progress:  st.Progress,
			Started:   st.Started,
			Completed: st.Completed,
			Tags:      st.Tags,
			Output:    st.Output,
		}
	}
	return res, total
}

// CancelStatement cancels a statement if it's waiting or running, immediately signalling the cancellation context.
func (s *Session) CancelStatement(id int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if id < 0 || id >= len(s.Statements) {
		return false
	}
	stmt := s.Statements[id]
	if stmt.State == StatementWaiting || stmt.State == StatementRunning {
		stmt.State = StatementCancelled
		if stmt.cancelFunc != nil {
			stmt.cancelFunc()
		}
		stmt.Output = &StatementOutput{
			Status:         "error",
			ExecutionCount: stmt.ID,
			Ename:          "StatementCancelled",
			Evalue:         "Statement was cancelled by user",
			Traceback:      []string{"Cancelled"},
		}
		s.Log = append(s.Log, "Statement execution cancelled by user")
		s.updateBusyStateLocked()
		return true
	}
	return false
}

// SetAppInfo updates the session's Application ID, Spark UI URLs, and Connect UI URLs in a thread-safe manner.
func (s *Session) SetAppInfo(appID string, sparkUI string, sparkHistory string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.AppID = appID
	if s.AppInfo == nil {
		s.AppInfo = make(map[string]string)
	}
	if appID != "" {
		s.AppInfo["sparkAppId"] = appID
	}
	if sparkUI != "" {
		sparkUI = strings.TrimRight(sparkUI, "/")
		s.AppInfo["sparkUiUrl"] = sparkUI
		if s.SessionID != "" {
			s.AppInfo["sparkConnectUiUrl"] = fmt.Sprintf("%s/connect/session/?id=%s", sparkUI, s.SessionID)
		} else {
			s.AppInfo["sparkConnectUiUrl"] = fmt.Sprintf("%s/connect/", sparkUI)
		}
	}
	if sparkHistory != "" && appID != "" {
		sparkHistory = strings.TrimRight(sparkHistory, "/")
		s.AppInfo["sparkHistoryUrl"] = fmt.Sprintf("%s/history/%s", sparkHistory, appID)
	}
}
