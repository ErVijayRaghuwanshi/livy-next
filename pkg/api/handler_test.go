package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"livy-next/pkg/api"
	"livy-next/pkg/session"
)

// MockSparkClient implements session.SparkClient for testing purposes.
type MockSparkClient struct {
	ExecuteFunc   func(ctx context.Context, sql string) (*session.QueryResult, error)
	CloseFunc     func() error
	GetAppIDFunc  func(ctx context.Context) (string, error)
	SetConfigFunc func(ctx context.Context, key string, value string) error
}

func (m *MockSparkClient) ExecuteSQL(ctx context.Context, sql string) (*session.QueryResult, error) {
	if m.ExecuteFunc != nil {
		return m.ExecuteFunc(ctx, sql)
	}
	return &session.QueryResult{
		Schema: []string{"a"},
		Data:   [][]interface{}{{1}},
	}, nil
}

func (m *MockSparkClient) GetAppID(ctx context.Context) (string, error) {
	if m.GetAppIDFunc != nil {
		return m.GetAppIDFunc(ctx)
	}
	return "mock-app-id-1234", nil
}

func (m *MockSparkClient) GetSessionID() string {
	return "mock-session-id-1234"
}

func (m *MockSparkClient) SetConfig(ctx context.Context, key string, value string) error {
	if m.SetConfigFunc != nil {
		return m.SetConfigFunc(ctx, key, value)
	}
	return nil
}

func (m *MockSparkClient) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

func TestLivyAPI(t *testing.T) {
	manager := session.NewManager(10 * time.Minute, 5 * time.Minute)
	defer manager.CloseAll()

	mockClient := &MockSparkClient{
		ExecuteFunc: func(ctx context.Context, sql string) (*session.QueryResult, error) {
			if sql == "FAIL" {
				return nil, errors.New("spark error")
			}
			if sql == "SESSION_CLOSED" {
				return nil, errors.New("execution error: [INVALID_HANDLE.SESSION_CLOSED] The handle is invalid.")
			}
			if sql == "BLOCK" {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(2 * time.Second):
					return nil, errors.New("block timeout")
				}
			}
			return &session.QueryResult{
				Schema: []string{"col1"},
				Data:   [][]interface{}{{"val1"}},
			}, nil
		},
	}

	creator := func(name string, kind string, conf map[string]string, jars []string, proxyUser string) (session.SparkClient, error) {
		return mockClient, nil
	}

	handler := api.NewHandler(manager, creator)
	router := api.SetupRouter(handler, []string{"http://example.com"})

	// 1. Create a Session & Verify CORS
	body := []byte(`{"kind": "spark"}`)
	req, _ := http.NewRequest("POST", "/sessions", bytes.NewBuffer(body))
	req.Header.Set("Origin", "http://example.com")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	assert.Equal(t, "http://example.com", rr.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, http.StatusCreated, rr.Code)
	var createdSession session.Session
	err := json.Unmarshal(rr.Body.Bytes(), &createdSession)
	assert.NoError(t, err)
	assert.Equal(t, 0, createdSession.ID)
	assert.Equal(t, session.SessionIdle, createdSession.State)

	// 2. List Sessions
	req, _ = http.NewRequest("GET", "/sessions", nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var listResp api.SessionsResponse
	err = json.Unmarshal(rr.Body.Bytes(), &listResp)
	assert.NoError(t, err)
	assert.Equal(t, 1, listResp.Total)
	assert.Equal(t, int64(600000), listResp.IdleTimeout)
	assert.Equal(t, int64(300000), listResp.DeadTimeout)
	assert.Equal(t, 0, listResp.Sessions[0].ID)

	// 3. Submit a Statement
	stmtBody := []byte(`{"code": "SELECT * FROM test"}`)
	req, _ = http.NewRequest("POST", "/sessions/0/statements", bytes.NewBuffer(stmtBody))
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusCreated, rr.Code)
	var stmt session.Statement
	err = json.Unmarshal(rr.Body.Bytes(), &stmt)
	assert.NoError(t, err)
	assert.Equal(t, 0, stmt.ID)
	assert.Equal(t, "SELECT * FROM test", stmt.Code)

	// Wait for execution to finish
	time.Sleep(100 * time.Millisecond)

	// 4. Retrieve the Statement Status and Output
	req, _ = http.NewRequest("GET", "/sessions/0/statements/0", nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var stmtStatus session.Statement
	err = json.Unmarshal(rr.Body.Bytes(), &stmtStatus)
	assert.NoError(t, err)
	assert.Equal(t, session.StatementAvailable, stmtStatus.State)
	assert.NotNil(t, stmtStatus.Output)
	assert.Equal(t, "ok", stmtStatus.Output.Status)

	// 5. Submit a Statement that Fails
	failBody := []byte(`{"code": "FAIL"}`)
	req, _ = http.NewRequest("POST", "/sessions/0/statements", bytes.NewBuffer(failBody))
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusCreated, rr.Code)
	err = json.Unmarshal(rr.Body.Bytes(), &stmt)
	assert.NoError(t, err)
	assert.Equal(t, 1, stmt.ID)

	time.Sleep(100 * time.Millisecond)

	req, _ = http.NewRequest("GET", "/sessions/0/statements/1", nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	err = json.Unmarshal(rr.Body.Bytes(), &stmtStatus)
	assert.NoError(t, err)
	assert.Equal(t, session.StatementError, stmtStatus.State)
	assert.Equal(t, "error", stmtStatus.Output.Status)
	assert.Contains(t, stmtStatus.Output.Evalue, "spark error")

	// 5b. Cancel a statement
	cancelBody := []byte(`{"code": "BLOCK"}`)
	req, _ = http.NewRequest("POST", "/sessions/0/statements", bytes.NewBuffer(cancelBody))
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusCreated, rr.Code)

	// Sleep briefly to let the execution start
	time.Sleep(20 * time.Millisecond)

	req, _ = http.NewRequest("POST", "/sessions/0/statements/2/cancel", nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.JSONEq(t, `{"msg": "cancelled"}`, rr.Body.String())

	// 5c. Submit a Statement that fails with SESSION_CLOSED
	closedBody := []byte(`{"code": "SESSION_CLOSED"}`)
	req, _ = http.NewRequest("POST", "/sessions/0/statements", bytes.NewBuffer(closedBody))
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusCreated, rr.Code)

	// Sleep briefly to let the execution run and call s.Close()
	time.Sleep(50 * time.Millisecond)

	// Verify session is now dead
	req, _ = http.NewRequest("GET", "/sessions/0", nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var sessionStatus session.Session
	err = json.Unmarshal(rr.Body.Bytes(), &sessionStatus)
	assert.NoError(t, err)
	assert.Equal(t, session.SessionDead, sessionStatus.State)

	// 6. Delete Session
	req, _ = http.NewRequest("DELETE", "/sessions/0", nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.JSONEq(t, `{"msg": "deleted"}`, rr.Body.String())

	// Verify session is dead but still queryable
	req, _ = http.NewRequest("GET", "/sessions/0", nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var deadSession session.Session
	err = json.Unmarshal(rr.Body.Bytes(), &deadSession)
	assert.NoError(t, err)
	assert.Equal(t, session.SessionDead, deadSession.State)

	// Verify submitting a statement to a dead session fails
	stmtBody = []byte(`{"code": "SELECT 1"}`)
	req, _ = http.NewRequest("POST", "/sessions/0/statements", bytes.NewBuffer(stmtBody))
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}
