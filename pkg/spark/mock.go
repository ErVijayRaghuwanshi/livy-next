package spark

import (
	"context"
	"livy-next/pkg/session"
)

// MockClient implements session.SparkClient for testing/mock mode.
type MockClient struct {
	AppName   string
	SessionID string
}

func (m *MockClient) ExecuteSQL(ctx context.Context, sql string) (*session.QueryResult, error) {
	return &session.QueryResult{
		Schema: session.Schema{
			Type: "struct",
			Fields: []session.SchemaField{
				{
					Name:     "mock_col",
					Type:     "string",
					Nullable: true,
					Metadata: make(map[string]interface{}),
				},
			},
		},
		Data: [][]interface{}{
			{"mock_result_data"},
		},
	}, nil
}

func (m *MockClient) GetAppID(ctx context.Context) (string, error) {
	if m.AppName != "" {
		return "mock-app-id-" + m.AppName, nil
	}
	return "mock-app-id-default", nil
}

func (m *MockClient) GetSessionID() string {
	if m.SessionID != "" {
		return m.SessionID
	}
	if m.AppName != "" {
		return "mock-session-id-" + m.AppName
	}
	return "mock-session-id-default"
}

func (m *MockClient) SetConfig(ctx context.Context, key string, value string) error {
	return nil
}

func (m *MockClient) Close() error {
	return nil
}
