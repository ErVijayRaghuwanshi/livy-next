package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"livy-next/pkg/session"
)

type dummySparkClient struct {
	execDelay time.Duration
	cancelled bool
}

func (d *dummySparkClient) ExecuteSQL(ctx context.Context, sql string) (*session.QueryResult, error) {
	if d.execDelay > 0 {
		select {
		case <-ctx.Done():
			d.cancelled = true
			return nil, ctx.Err()
		case <-time.After(d.execDelay):
		}
	}
	return &session.QueryResult{
		Schema: session.Schema{
			Type: "struct",
			Fields: []session.SchemaField{
				{Name: "id", Type: "integer"},
				{Name: "val", Type: "string"},
			},
		},
		Data: [][]interface{}{
			{1, "first"},
			{2, "second"},
			{3, "third"},
			{4, "fourth"},
			{5, "fifth"},
		},
	}, nil
}

func (d *dummySparkClient) GetAppID(ctx context.Context) (string, error) {
	return "app-12345", nil
}

func (d *dummySparkClient) GetSessionID() string {
	return "sess-uuid-12345"
}

func (d *dummySparkClient) SetConfig(ctx context.Context, key string, value string) error {
	return nil
}

func (d *dummySparkClient) Close() error {
	return nil
}

func TestSession_IdentityAndAppInfo(t *testing.T) {
	params := session.SessionCreateParams{
		Name:      "test-session",
		Kind:      "spark",
		UserID:    "ervijay",
		SessionID: "my-custom-uuid",
		UserAgent: "jupyter-notebook",
		ProxyUser: "proxy_ervijay",
	}

	client := &dummySparkClient{}
	sess := session.NewSession(1, params, client)

	assert.Equal(t, 1, sess.ID)
	assert.Equal(t, "test-session", sess.Name)
	assert.Equal(t, "ervijay", sess.UserID)
	assert.Equal(t, "my-custom-uuid", sess.SessionID)
	assert.Equal(t, "jupyter-notebook", sess.UserAgent)

	sess.SetAppInfo("app-12345", "http://localhost:4040", "http://localhost:18088")
	assert.Equal(t, "http://localhost:4040", sess.AppInfo["sparkUiUrl"])
	assert.Equal(t, "http://localhost:4040/connect/session/?id=my-custom-uuid", sess.AppInfo["sparkConnectUiUrl"])
	assert.Equal(t, "http://localhost:18088/history/app-12345", sess.AppInfo["sparkHistoryUrl"])
}

func TestSession_ResultPaginationAndOutputRetention(t *testing.T) {
	params := session.SessionCreateParams{
		Name: "paging-session",
	}
	client := &dummySparkClient{}
	sess := session.NewSession(2, params, client)

	stmt := sess.SubmitStatement("SELECT * FROM test", "tag1", "tag2")
	assert.Equal(t, []string{"tag1", "tag2"}, stmt.Tags)

	// Wait for execution to finish
	time.Sleep(50 * time.Millisecond)

	// 1. Fetch full statement
	fullStmt, exists := sess.GetStatement(stmt.ID, 0, 0)
	assert.True(t, exists)
	assert.Equal(t, session.StatementAvailable, fullStmt.State)
	qr, ok := fullStmt.Output.Data["application/json"].(*session.QueryResult)
	assert.True(t, ok)
	assert.Len(t, qr.Data, 5)

	// 2. Fetch page 1 (from=0, size=2)
	p1Stmt, _ := sess.GetStatement(stmt.ID, 0, 2)
	p1QR, ok := p1Stmt.Output.Data["application/json"].(*session.QueryResult)
	assert.True(t, ok)
	assert.Len(t, p1QR.Data, 2)
	assert.Equal(t, 5, p1QR.Total)
	assert.Equal(t, 0, p1QR.From)
	assert.Equal(t, 2, p1QR.Size)
	assert.Equal(t, 1, p1QR.Data[0][0])
	assert.Equal(t, 2, p1QR.Data[1][0])

	// 3. Fetch page 2 (from=2, size=2)
	p2Stmt, _ := sess.GetStatement(stmt.ID, 2, 2)
	p2QR, ok := p2Stmt.Output.Data["application/json"].(*session.QueryResult)
	assert.True(t, ok)
	assert.Len(t, p2QR.Data, 2)
	assert.Equal(t, 5, p2QR.Total)
	assert.Equal(t, 2, p2QR.From)
	assert.Equal(t, 2, p2QR.Size)
	assert.Equal(t, 3, p2QR.Data[0][0])
	assert.Equal(t, 4, p2QR.Data[1][0])

	// 4. Verify output in session is STILL available (was NOT cleared!)
	againStmt, _ := sess.GetStatement(stmt.ID, 0, 0)
	assert.NotNil(t, againStmt.Output)
}

func TestSession_CancellationForwarding(t *testing.T) {
	client := &dummySparkClient{execDelay: 200 * time.Millisecond}
	sess := session.NewSession(3, session.SessionCreateParams{}, client)

	stmt := sess.SubmitStatement("SELECT sleep(10)")
	time.Sleep(20 * time.Millisecond) // let runStatement start

	cancelled := sess.CancelStatement(stmt.ID)
	assert.True(t, cancelled)

	time.Sleep(50 * time.Millisecond)
	assert.True(t, client.cancelled, "Underlying context should be cancelled")

	finalStmt, _ := sess.GetStatement(stmt.ID, 0, 0)
	assert.Equal(t, session.StatementCancelled, finalStmt.State)
}
