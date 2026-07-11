package spark

import (
	"context"
	"reflect"
	"unsafe"

	"github.com/apache/spark-connect-go/spark/sql"
	"livy-next/pkg/session"
)

type Client struct {
	session sql.SparkSession
}

func NewClient(ctx context.Context, remote string, appName string) (*Client, error) {
	sparkSession, err := sql.NewSessionBuilder().Remote(remote).Build(ctx)
	if err != nil {
		return nil, err
	}
	c := &Client{session: sparkSession}
	if appName != "" {
		_ = c.SetConfig(ctx, "spark.app.name", appName)
	}
	return c, nil
}

func (c *Client) ExecuteSQL(ctx context.Context, sqlQuery string) (*session.QueryResult, error) {
	df, err := c.session.Sql(ctx, sqlQuery)
	if err != nil {
		return nil, err
	}

	rows, err := df.Collect(ctx)
	if err != nil {
		return nil, err
	}

	var queryResult session.QueryResult
	queryResult.Data = make([][]interface{}, 0, len(rows))

	if len(rows) > 0 {
		firstRow := rows[0]
		names := firstRow.FieldNames()

		for _, row := range rows {
			rowValues := make([]interface{}, len(names))
			for i, name := range names {
				rowValues[i] = row.Value(name)
			}
			queryResult.Data = append(queryResult.Data, rowValues)
		}

		// Build the schema in the StructType layout expected by Livy clients
		var fields []session.SchemaField
		for _, name := range names {
			val := firstRow.Value(name)
			fields = append(fields, session.SchemaField{
				Name:     name,
				Type:     detectSparkType(val),
				Nullable: true,
				Metadata: make(map[string]interface{}),
			})
		}
		queryResult.Schema = session.Schema{
			Type:   "struct",
			Fields: fields,
		}
	}

	return &queryResult, nil
}

func (c *Client) GetAppID(ctx context.Context) (string, error) {
	return c.session.Config().Get(ctx, "spark.app.id")
}

func (c *Client) GetSessionID() string {
	defer func() {
		_ = recover()
	}()
	valSession := reflect.ValueOf(c.session).Elem()
	clientField := valSession.FieldByName("client")
	if !clientField.IsValid() {
		return ""
	}
	clientVal := reflect.NewAt(clientField.Type(), unsafe.Pointer(clientField.UnsafeAddr())).Elem()
	valClientImpl := reflect.ValueOf(clientVal.Interface()).Elem()
	sessionIdField := valClientImpl.FieldByName("sessionId")
	if !sessionIdField.IsValid() {
		return ""
	}
	return sessionIdField.String()
}

func (c *Client) SetConfig(ctx context.Context, key string, value string) error {
	return c.session.Config().Set(ctx, key, value)
}

func (c *Client) Close() error {
	_ = c.session.Stop()
	releaseSparkSession(context.Background(), c.session)
	return nil
}

func releaseSparkSession(ctx context.Context, session sql.SparkSession) {
	defer func() {
		// Recover silently from any panic to avoid crashing the program if internal library structs change
		_ = recover()
	}()

	valSession := reflect.ValueOf(session).Elem()
	clientField := valSession.FieldByName("client")
	if !clientField.IsValid() {
		return
	}
	clientVal := reflect.NewAt(clientField.Type(), unsafe.Pointer(clientField.UnsafeAddr())).Elem()

	valClientImpl := reflect.ValueOf(clientVal.Interface()).Elem()

	sessionIdField := valClientImpl.FieldByName("sessionId")
	if !sessionIdField.IsValid() {
		return
	}
	sessionId := sessionIdField.String()

	optsField := valClientImpl.FieldByName("opts")
	if !optsField.IsValid() {
		return
	}
	optsVal := reflect.NewAt(optsField.Type(), unsafe.Pointer(optsField.UnsafeAddr())).Elem()
	userId := optsVal.FieldByName("UserId").String()
	userAgent := optsVal.FieldByName("UserAgent").String()

	rpcClientField := valClientImpl.FieldByName("client")
	if !rpcClientField.IsValid() {
		return
	}
	rpcClientVal := reflect.NewAt(rpcClientField.Type(), unsafe.Pointer(rpcClientField.UnsafeAddr())).Elem()

	method := rpcClientVal.MethodByName("ReleaseSession")
	if !method.IsValid() {
		return
	}

	reqType := method.Type().In(1) // *generated.ReleaseSessionRequest
	reqVal := reflect.New(reqType.Elem())
	reqElem := reqVal.Elem()

	reqElem.FieldByName("SessionId").SetString(sessionId)

	userContextField := reqElem.FieldByName("UserContext")
	userContextType := userContextField.Type() // *generated.UserContext
	userContextVal := reflect.New(userContextType.Elem())
	userContextVal.Elem().FieldByName("UserId").SetString(userId)
	userContextField.Set(userContextVal)

	clientTypeField := reqElem.FieldByName("ClientType")
	clientTypeType := clientTypeField.Type() // *string
	clientTypeVal := reflect.New(clientTypeType.Elem())
	clientTypeVal.Elem().SetString(userAgent)
	clientTypeField.Set(clientTypeVal)

	// Call ReleaseSession(ctx, req)
	method.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reqVal,
	})
}

func detectSparkType(val interface{}) string {
	if val == nil {
		return "void"
	}
	switch val.(type) {
	case int, int32, int64:
		return "integer"
	case float32, float64:
		return "double"
	case bool:
		return "boolean"
	case string:
		return "string"
	default:
		return "string"
	}
}
