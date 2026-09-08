package session

import (
	"context"
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

// QueryResult represents the parsed results of a Spark SQL statement.
type QueryResult struct {
	Schema interface{}     `json:"schema"`
	Data   [][]interface{} `json:"data"`
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
}

// Session represents an interactive Livy session.
type Session struct {
	ID           int               `json:"id"`
	Name         string            `json:"name,omitempty"`
	AppID        string            `json:"appId,omitempty"`
	SessionID    string            `json:"sessionId,omitempty"`
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
func NewSession(id int, name string, kind string, client SparkClient) *Session {
	var sessionId string
	if client != nil {
		sessionId = client.GetSessionID()
	}
	return &Session{
		ID:           id,
		Name:         name,
		State:        SessionStarting,
		Kind:         kind,
		SessionID:    sessionId,
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
func (s *Session) SubmitStatement(code string) *Statement {
	s.mu.Lock()
	defer s.mu.Unlock()

	stmtID := len(s.Statements)
	stmt := &Statement{
		ID:       stmtID,
		Code:     code,
		State:    StatementWaiting,
		Progress: 0.0,
		Started:  time.Now().UnixNano() / int64(time.Millisecond),
	}
	s.Statements = append(s.Statements, stmt)
	s.State = SessionBusy
	s.LastActivity = time.Now()

	// Execute asynchronously
	go s.runStatement(stmt)

	return stmt
}

func (s *Session) runStatement(stmt *Statement) {
	s.mu.Lock()
	stmt.State = StatementRunning
	stmt.Progress = 0.1
	s.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	// Update session state back to idle if there are no other statements running
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
	s.LastActivity = time.Now()
}

// GetStatement retrieves a specific statement by ID. If the statement is in a terminal state
// (available, error, or cancelled), we return a copy containing the output and clear the
// output in session storage to avoid keeping results in memory.
func (s *Session) GetStatement(id int) (*Statement, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id < 0 || id >= len(s.Statements) {
		return nil, false
	}
	stmt := s.Statements[id]
	if stmt.State == StatementAvailable || stmt.State == StatementError || stmt.State == StatementCancelled {
		if stmt.Output != nil {
			// Copy the statement to return with the output
			stmtCopy := &Statement{
				ID:        stmt.ID,
				Code:      stmt.Code,
				State:     stmt.State,
				Output:    stmt.Output,
				Progress:  stmt.Progress,
				Started:   stmt.Started,
				Completed: stmt.Completed,
			}
			// Discard the output from session storage to free resources
			stmt.Output = nil
			return stmtCopy, true
		}
	}
	return stmt, true
}

// GetStatements retrieves all statements.
func (s *Session) GetStatements() []*Statement {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Statements
}

// CancelStatement cancels a statement if it's waiting or running.
func (s *Session) CancelStatement(id int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if id < 0 || id >= len(s.Statements) {
		return false
	}
	stmt := s.Statements[id]
	if stmt.State == StatementWaiting || stmt.State == StatementRunning {
		stmt.State = StatementCancelled
		stmt.Output = &StatementOutput{
			Status:         "error",
			ExecutionCount: stmt.ID,
			Ename:          "StatementCancelled",
			Evalue:         "Statement was cancelled by user",
			Traceback:      []string{"Cancelled"},
		}
		s.Log = append(s.Log, "Statement execution cancelled by user")
		return true
	}
	return false
}

// SetAppInfo updates the session's Application ID and metadata in a thread-safe manner.
func (s *Session) SetAppInfo(appID string, uiURL string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.AppID = appID
	if s.AppInfo == nil {
		s.AppInfo = make(map[string]string)
	}
	s.AppInfo["sparkUiUrl"] = uiURL
}
