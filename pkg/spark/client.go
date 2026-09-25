package spark

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/spark-connect-go/spark/sql"
	"livy-next/pkg/session"
)

type Client struct {
	session sql.SparkSession
	maxRows int
}

func NewClient(ctx context.Context, remote string, appName string, maxRows ...int) (*Client, error) {
	limit := 10000
	if len(maxRows) > 0 && maxRows[0] > 0 {
		limit = maxRows[0]
	}
	sparkSession, err := sql.NewSessionBuilder().Remote(remote).Build(ctx)
	if err != nil {
		return nil, err
	}
	c := &Client{session: sparkSession, maxRows: limit}
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

	// Limit execution results to prevent OOM crashes (configurable via maxRows)
	if c.maxRows > 0 {
		df = df.Limit(ctx, int32(c.maxRows))
	}

	tblPtr, err := df.ToArrow(ctx)
	if err != nil {
		return nil, err
	}
	if tblPtr == nil {
		return &session.QueryResult{
			Data: make([][]interface{}, 0),
		}, nil
	}
	tbl := *tblPtr
	defer tbl.Release()

	return ArrowTableToQueryResult(tbl)
}

func (c *Client) GetAppID(ctx context.Context) (string, error) {
	return c.session.Config().Get(ctx, "spark.app.id")
}

// GetSessionTimeout fetches the server-side idle session timeout from Spark Connect.
// Returns parsed time.Duration, or error if not configured or query fails.
func (c *Client) GetSessionTimeout(ctx context.Context) (time.Duration, error) {
	val, err := c.session.Config().Get(ctx, "spark.connect.session.manager.defaultSessionTimeout")
	if err != nil {
		return 0, err
	}
	return ParseSparkDuration(val)
}

// GetMaintenanceInterval fetches the session maintenance reap interval from Spark Connect.
func (c *Client) GetMaintenanceInterval(ctx context.Context) (time.Duration, error) {
	val, err := c.session.Config().Get(ctx, "spark.connect.session.manager.maintenanceInterval")
	if err != nil {
		return 0, err
	}
	return ParseSparkDuration(val)
}

// GetSparkVersion queries Spark Connect for its exact runtime version string (e.g. "4.2.0").
func (c *Client) GetSparkVersion(ctx context.Context) (string, error) {
	qr, err := c.ExecuteSQL(ctx, "SELECT version()")
	if err != nil {
		return "", err
	}
	if len(qr.Data) > 0 && len(qr.Data[0]) > 0 {
		raw := strings.TrimSpace(fmt.Sprintf("%v", qr.Data[0][0]))
		parts := strings.Split(raw, " ")
		if len(parts) > 0 && parts[0] != "" {
			return parts[0], nil
		}
		return raw, nil
	}
	return "", fmt.Errorf("empty version response from spark connect")
}

// GetMaster queries Spark Connect for the Spark Master cluster endpoint.
func (c *Client) GetMaster(ctx context.Context) (string, error) {
	return c.session.Config().Get(ctx, "spark.master")
}

var dayRegex = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)d$`)

// ParseSparkDuration parses duration strings accepted by Apache Spark.
// Supports units like "120m", "2h", "30s", "500ms", "1d", "2.5d".
// Returns -1ns for disabled timeouts ("-1", "-1ms", "-1s", "-1m").
func ParseSparkDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0, fmt.Errorf("empty duration string")
	}
	if s == "-1" || s == "-1ms" || s == "-1s" || s == "-1m" {
		return -1 * time.Nanosecond, nil
	}

	// Handle day unit "d", e.g., "1d", "7d", "0.5d"
	if matches := dayRegex.FindStringSubmatch(s); len(matches) == 2 {
		days, err := strconv.ParseFloat(matches[1], 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse days in %q: %w", s, err)
		}
		return time.Duration(days * 24 * float64(time.Hour)), nil
	}

	// Standard Go time.ParseDuration (supports ns, us, µs, ms, s, m, h)
	d, err := time.ParseDuration(s)
	if err == nil {
		return d, nil
	}

	// Fallback for pure numeric strings without unit: treat as milliseconds
	if val, errNum := strconv.ParseInt(s, 10, 64); errNum == nil {
		return time.Duration(val) * time.Millisecond, nil
	}

	return 0, fmt.Errorf("invalid spark duration %q: %w", s, err)
}

// ExtractHostFromRemote extracts the hostname or IP address from a Spark Connect remote URI.
// Handles formats like "sc://localhost:15002", "sc://spark-master:15002/;session_id=...", "http://127.0.0.1:4040".
func ExtractHostFromRemote(remote string) string {
	clean := strings.TrimSpace(remote)
	clean = strings.TrimPrefix(clean, "sc://")
	clean = strings.TrimPrefix(clean, "spark://")
	clean = strings.TrimPrefix(clean, "http://")
	clean = strings.TrimPrefix(clean, "https://")
	// Strip semicolon parameters if present
	if idx := strings.Index(clean, ";"); idx != -1 {
		clean = clean[:idx]
	}
	clean = strings.TrimRight(clean, "/")
	host, _, err := net.SplitHostPort(clean)
	if err == nil && host != "" {
		return host
	}
	parts := strings.Split(clean, ":")
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}
	return "localhost"
}

// DiscoverSparkUIEndpoint probes the connected Spark Connect instance to find its active Web UI port.
// Probes candidate ports [remotePort, 4040, 4041, 4141, 4042] on the remote host with a fast HTTP request.
// Returns the reachable endpoint URL (e.g. "http://localhost:4040"), or fallback to default port 4040.
func DiscoverSparkUIEndpoint(remote string) string {
	clean := strings.TrimSpace(remote)
	clean = strings.TrimPrefix(clean, "sc://")
	clean = strings.TrimPrefix(clean, "spark://")
	clean = strings.TrimPrefix(clean, "http://")
	clean = strings.TrimPrefix(clean, "https://")
	if idx := strings.Index(clean, ";"); idx != -1 {
		clean = clean[:idx]
	}
	clean = strings.TrimRight(clean, "/")

	host, portStr, _ := net.SplitHostPort(clean)
	if host == "" || host == "0.0.0.0" {
		if host == "" {
			host = ExtractHostFromRemote(remote)
		} else {
			host = "127.0.0.1"
		}
	}

	var candidatePorts []int
	if portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil && p > 0 {
			candidatePorts = append(candidatePorts, p)
		}
	}
	candidatePorts = append(candidatePorts, 4040, 4041, 4141, 4042)

	client := &http.Client{
		Timeout: 250 * time.Millisecond,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // don't follow redirects, 302 means server is up
		},
	}

	for _, port := range candidatePorts {
		testURL := fmt.Sprintf("http://%s:%d/connect/", host, port)
		resp, err := client.Get(testURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusFound {
				return fmt.Sprintf("http://%s:%d", host, port)
			}
		}
		rootURL := fmt.Sprintf("http://%s:%d/", host, port)
		respRoot, errRoot := client.Get(rootURL)
		if errRoot == nil {
			respRoot.Body.Close()
			if respRoot.StatusCode == http.StatusOK || respRoot.StatusCode == http.StatusFound {
				return fmt.Sprintf("http://%s:%d", host, port)
			}
		}
	}

	return fmt.Sprintf("http://%s:4040", host)
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

// SanitizeValue converts arbitrary Spark/Arrow types and map[interface{}]interface{} into JSON-serializable types.
func SanitizeValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case bool, string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return v

	case []byte:
		return base64.StdEncoding.EncodeToString(v)

	case decimal128.Num:
		return v.BigInt().String()

	case decimal256.Num:
		return v.BigInt().String()

	case arrow.Timestamp:
		epochUs := int64(v)
		t := time.Unix(epochUs/1000000, (epochUs%1000000)*1000).UTC()
		return t.Format(time.RFC3339)

	case arrow.Date32:
		epochDays := int64(v)
		epochTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(epochDays))
		return epochTime.Format("2006-01-02")

	case arrow.Date64:
		epochMs := int64(v)
		t := time.Unix(epochMs/1000, (epochMs%1000)*1000000).UTC()
		return t.Format("2006-01-02")

	case time.Time:
		if v.IsZero() {
			return nil
		}
		return v.Format(time.RFC3339)

	case arrow.MonthInterval:
		return formatMonthInterval(int32(v))

	case arrow.DayTimeInterval:
		totalSec := v.Milliseconds / 1000
		millis := v.Milliseconds % 1000
		hours := totalSec / 3600
		mins := (totalSec % 3600) / 60
		secs := totalSec % 60
		if millis > 0 {
			return fmt.Sprintf("INTERVAL '%d %02d:%02d:%02d.%03d' DAY TO SECOND", v.Days, hours, mins, secs, millis)
		}
		return fmt.Sprintf("INTERVAL '%d %02d:%02d:%02d' DAY TO SECOND", v.Days, hours, mins, secs)

	case arrow.MonthDayNanoInterval:
		return fmt.Sprintf("%d months %d days %d ns", v.Months, v.Days, v.Nanoseconds)

	case arrow.Duration:
		return formatDuration(int64(v), arrow.Microsecond)

	case []interface{}:
		res := make([]interface{}, len(v))
		for i, item := range v {
			res[i] = SanitizeValue(item)
		}
		return res

	case map[string]interface{}:
		res := make(map[string]interface{}, len(v))
		for k, val := range v {
			res[k] = SanitizeValue(val)
		}
		return res

	case map[interface{}]interface{}:
		res := make(map[string]interface{}, len(v))
		for k, val := range v {
			keyStr := fmt.Sprintf("%v", k)
			res[keyStr] = SanitizeValue(val)
		}
		return res

	default:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Pointer, reflect.Interface:
			if rv.IsNil() {
				return nil
			}
			return SanitizeValue(rv.Elem().Interface())

		case reflect.Slice, reflect.Array:
			if rv.Type().Elem().Kind() == reflect.Uint8 {
				length := rv.Len()
				bytes := make([]byte, length)
				for i := 0; i < length; i++ {
					bytes[i] = byte(rv.Index(i).Uint())
				}
				return base64.StdEncoding.EncodeToString(bytes)
			}
			length := rv.Len()
			res := make([]interface{}, length)
			for i := 0; i < length; i++ {
				res[i] = SanitizeValue(rv.Index(i).Interface())
			}
			return res

		case reflect.Map:
			res := make(map[string]interface{}, rv.Len())
			for _, key := range rv.MapKeys() {
				keyStr := fmt.Sprintf("%v", key.Interface())
				val := rv.MapIndex(key)
				res[keyStr] = SanitizeValue(val.Interface())
			}
			return res

		case reflect.Struct:
			if m, ok := value.(json.Marshaler); ok {
				return m
			}
			if s, ok := value.(fmt.Stringer); ok {
				return s.String()
			}
			res := make(map[string]interface{})
			typ := rv.Type()
			for i := 0; i < rv.NumField(); i++ {
				field := typ.Field(i)
				if field.PkgPath == "" { // exported field
					res[field.Name] = SanitizeValue(rv.Field(i).Interface())
				}
			}
			return res

		default:
			return value
		}
	}
}

func detectSparkType(val interface{}) string {
	if val == nil {
		return "void"
	}
	switch val.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "integer"
	case float32, float64, decimal128.Num, decimal256.Num:
		return "double"
	case bool:
		return "boolean"
	case string:
		return "string"
	case arrow.Date32, arrow.Date64:
		return "date"
	case arrow.Timestamp, time.Time:
		return "timestamp"
	case arrow.MonthInterval, arrow.DayTimeInterval, arrow.MonthDayNanoInterval, arrow.Duration:
		return "interval"
	case map[string]interface{}, map[interface{}]interface{}:
		return "map"
	case []interface{}:
		return "array"
	default:
		rv := reflect.ValueOf(val)
		switch rv.Kind() {
		case reflect.Map:
			return "map"
		case reflect.Slice, reflect.Array:
			return "array"
		case reflect.Struct:
			return "struct"
		default:
			return "string"
		}
	}
}

// ArrowTableToQueryResult converts an arrow.Table into a session.QueryResult with accurate schema and data types.
func ArrowTableToQueryResult(tbl arrow.Table) (*session.QueryResult, error) {
	numCols := int(tbl.NumCols())
	numRows := int(tbl.NumRows())

	var queryResult session.QueryResult
	queryResult.Data = make([][]interface{}, numRows)
	for i := 0; i < numRows; i++ {
		queryResult.Data[i] = make([]interface{}, numCols)
	}

	if numCols > 0 {
		fields := make([]session.SchemaField, numCols)
		for i := 0; i < numCols; i++ {
			f := tbl.Schema().Field(i)
			fields[i] = session.SchemaField{
				Name:     f.Name,
				Type:     detectSparkTypeFromArrow(f.Type),
				Nullable: f.Nullable,
				Metadata: make(map[string]interface{}),
			}
		}
		queryResult.Schema = session.Schema{
			Type:   "struct",
			Fields: fields,
		}

		for colIdx := 0; colIdx < numCols; colIdx++ {
			col := tbl.Column(colIdx)
			rowOffset := 0
			for _, chunk := range col.Data().Chunks() {
				chunkLen := chunk.Len()
				for i := 0; i < chunkLen; i++ {
					queryResult.Data[rowOffset+i][colIdx] = extractArrowValue(chunk, i)
				}
				rowOffset += chunkLen
			}
		}
	}

	return &queryResult, nil
}

func detectSparkTypeFromArrow(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.BOOL:
		return "boolean"
	case arrow.INT8:
		return "byte"
	case arrow.INT16:
		return "short"
	case arrow.INT32:
		return "integer"
	case arrow.INT64:
		return "long"
	case arrow.UINT8:
		return "byte"
	case arrow.UINT16:
		return "short"
	case arrow.UINT32:
		return "integer"
	case arrow.UINT64:
		return "long"
	case arrow.FLOAT16, arrow.FLOAT32:
		return "float"
	case arrow.FLOAT64:
		return "double"
	case arrow.DECIMAL128, arrow.DECIMAL256, arrow.DECIMAL32, arrow.DECIMAL64:
		return "decimal"
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "string"
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
		return "binary"
	case arrow.DATE32, arrow.DATE64:
		return "date"
	case arrow.TIMESTAMP:
		return "timestamp"
	case arrow.INTERVAL_MONTHS, arrow.INTERVAL_DAY_TIME, arrow.INTERVAL_MONTH_DAY_NANO, arrow.DURATION:
		return "interval"
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST, arrow.LIST_VIEW, arrow.LARGE_LIST_VIEW:
		return "array"
	case arrow.MAP:
		return "map"
	case arrow.STRUCT:
		return "struct"
	default:
		return "string"
	}
}

func extractArrowValue(arr arrow.Array, i int) interface{} {
	if arr.IsNull(i) {
		return nil
	}

	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(i)
	case *array.Int8:
		return a.Value(i)
	case *array.Int16:
		return a.Value(i)
	case *array.Int32:
		return a.Value(i)
	case *array.Int64:
		return a.Value(i)
	case *array.Uint8:
		return a.Value(i)
	case *array.Uint16:
		return a.Value(i)
	case *array.Uint32:
		return a.Value(i)
	case *array.Uint64:
		return a.Value(i)
	case *array.Float16:
		return a.Value(i).Float32()
	case *array.Float32:
		return a.Value(i)
	case *array.Float64:
		return a.Value(i)
	case *array.String:
		return a.Value(i)
	case *array.LargeString:
		return a.Value(i)
	case *array.StringView:
		return a.Value(i)
	case *array.Binary:
		return base64.StdEncoding.EncodeToString(a.Value(i))
	case *array.LargeBinary:
		return base64.StdEncoding.EncodeToString(a.Value(i))
	case *array.BinaryView:
		return base64.StdEncoding.EncodeToString(a.Value(i))
	case *array.FixedSizeBinary:
		return base64.StdEncoding.EncodeToString(a.Value(i))
	case *array.Date32:
		epochDays := int64(a.Value(i))
		epochTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(epochDays))
		return epochTime.Format("2006-01-02")
	case *array.Date64:
		epochMs := int64(a.Value(i))
		t := time.Unix(epochMs/1000, (epochMs%1000)*1000000).UTC()
		return t.Format("2006-01-02")
	case *array.Timestamp:
		unit := a.DataType().(*arrow.TimestampType).Unit
		ts := a.Value(i)
		t := ts.ToTime(unit).UTC()
		return t.Format(time.RFC3339)
	case *array.MonthInterval:
		return formatMonthInterval(int32(a.Value(i)))
	case *array.DayTimeInterval:
		dt := a.Value(i)
		totalSec := dt.Milliseconds / 1000
		millis := dt.Milliseconds % 1000
		hours := totalSec / 3600
		mins := (totalSec % 3600) / 60
		secs := totalSec % 60
		if millis > 0 {
			return fmt.Sprintf("INTERVAL '%d %02d:%02d:%02d.%03d' DAY TO SECOND", dt.Days, hours, mins, secs, millis)
		}
		return fmt.Sprintf("INTERVAL '%d %02d:%02d:%02d' DAY TO SECOND", dt.Days, hours, mins, secs)
	case *array.MonthDayNanoInterval:
		v := a.Value(i)
		return fmt.Sprintf("%d months %d days %d ns", v.Months, v.Days, v.Nanoseconds)
	case *array.Duration:
		unit := a.DataType().(*arrow.DurationType).Unit
		return formatDuration(int64(a.Value(i)), unit)
	case *array.Decimal128:
		v := a.Value(i)
		scale := a.DataType().(*arrow.Decimal128Type).Scale
		return decimal128ToString(v, scale)
	case *array.Decimal256:
		v := a.Value(i)
		scale := a.DataType().(*arrow.Decimal256Type).Scale
		return decimal256ToString(v, scale)
	case *array.List:
		raw := a.GetOneForMarshal(i)
		if rm, ok := raw.(json.RawMessage); ok {
			var val interface{}
			if err := json.Unmarshal(rm, &val); err == nil {
				return SanitizeValue(val)
			}
		}
		return SanitizeValue(raw)
	case *array.LargeList:
		raw := a.GetOneForMarshal(i)
		if rm, ok := raw.(json.RawMessage); ok {
			var val interface{}
			if err := json.Unmarshal(rm, &val); err == nil {
				return SanitizeValue(val)
			}
		}
		return SanitizeValue(raw)
	case *array.FixedSizeList:
		raw := a.GetOneForMarshal(i)
		if rm, ok := raw.(json.RawMessage); ok {
			var val interface{}
			if err := json.Unmarshal(rm, &val); err == nil {
				return SanitizeValue(val)
			}
		}
		return SanitizeValue(raw)
	case *array.Map:
		raw := a.GetOneForMarshal(i)
		if rm, ok := raw.(json.RawMessage); ok {
			var kvs []struct {
				Key   interface{} `json:"key"`
				Value interface{} `json:"value"`
			}
			if err := json.Unmarshal(rm, &kvs); err == nil {
				m := make(map[string]interface{}, len(kvs))
				for _, kv := range kvs {
					m[fmt.Sprintf("%v", kv.Key)] = SanitizeValue(kv.Value)
				}
				return m
			}
		}
		return SanitizeValue(raw)
	case *array.Struct:
		raw := a.GetOneForMarshal(i)
		if m, ok := raw.(map[string]interface{}); ok {
			return SanitizeValue(m)
		}
		if rm, ok := raw.(json.RawMessage); ok {
			var val map[string]interface{}
			if err := json.Unmarshal(rm, &val); err == nil {
				return SanitizeValue(val)
			}
		}
		return SanitizeValue(raw)
	default:
		raw := arr.GetOneForMarshal(i)
		if rm, ok := raw.(json.RawMessage); ok {
			var val interface{}
			if err := json.Unmarshal(rm, &val); err == nil {
				return SanitizeValue(val)
			}
			return string(rm)
		}
		return SanitizeValue(raw)
	}
}

func formatMonthInterval(m int32) string {
	sign := ""
	if m < 0 {
		sign = "-"
		m = -m
	}
	years := m / 12
	months := m % 12
	return fmt.Sprintf("INTERVAL '%s%d-%d' YEAR TO MONTH", sign, years, months)
}

func formatDuration(duration int64, unit arrow.TimeUnit) string {
	var micros int64
	switch unit {
	case arrow.Second:
		micros = duration * 1000000
	case arrow.Millisecond:
		micros = duration * 1000
	case arrow.Microsecond:
		micros = duration
	case arrow.Nanosecond:
		micros = duration / 1000
	default:
		micros = duration
	}

	sign := ""
	if micros < 0 {
		sign = "-"
		micros = -micros
	}

	totalSec := micros / 1000000
	microRem := micros % 1000000

	days := totalSec / 86400
	secRem := totalSec % 86400
	hours := secRem / 3600
	minRem := secRem % 3600
	minutes := minRem / 60
	seconds := minRem % 60

	if microRem > 0 {
		return fmt.Sprintf("INTERVAL '%s%d %02d:%02d:%02d.%06d' DAY TO SECOND", sign, days, hours, minutes, seconds, microRem)
	}
	return fmt.Sprintf("INTERVAL '%s%d %02d:%02d:%02d' DAY TO SECOND", sign, days, hours, minutes, seconds)
}

func decimal128ToString(num decimal128.Num, scale int32) string {
	b := num.BigInt()
	str := b.String()
	if scale <= 0 {
		return str
	}
	neg := false
	if len(str) > 0 && str[0] == '-' {
		neg = true
		str = str[1:]
	}
	for int32(len(str)) <= scale {
		str = "0" + str
	}
	dotPos := len(str) - int(scale)
	res := str[:dotPos] + "." + str[dotPos:]
	if neg {
		res = "-" + res
	}
	return res
}

func decimal256ToString(num decimal256.Num, scale int32) string {
	b := num.BigInt()
	str := b.String()
	if scale <= 0 {
		return str
	}
	neg := false
	if len(str) > 0 && str[0] == '-' {
		neg = true
		str = str[1:]
	}
	for int32(len(str)) <= scale {
		str = "0" + str
	}
	dotPos := len(str) - int(scale)
	res := str[:dotPos] + "." + str[dotPos:]
	if neg {
		res = "-" + res
	}
	return res
}
