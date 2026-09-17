package spark

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"livy-next/pkg/session"
)

func TestSanitizeValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{
			name:  "primitive int",
			input: 42,
			expected: 42,
		},
		{
			name:  "primitive string",
			input: "hello",
			expected: "hello",
		},
		{
			name:  "map[interface{}]interface{}",
			input: map[interface{}]interface{}{"key1": "val1", 2: "val2"},
			expected: map[string]interface{}{"key1": "val1", "2": "val2"},
		},
		{
			name: "nested map[interface{}]interface{}",
			input: map[interface{}]interface{}{
				"outer": map[interface{}]interface{}{
					"innerKey": 123,
				},
				"array": []interface{}{
					map[interface{}]interface{}{"arrKey": true},
				},
			},
			expected: map[string]interface{}{
				"outer": map[string]interface{}{
					"innerKey": 123,
				},
				"array": []interface{}{
					map[string]interface{}{"arrKey": true},
				},
			},
		},
		{
			name:     "arrow.Date32",
			input:    arrow.Date32(19000), // ~2022
			expected: "2022-01-08",
		},
		{
			name:     "arrow.MonthInterval",
			input:    arrow.MonthInterval(24),
			expected: "INTERVAL '2-0' YEAR TO MONTH",
		},
		{
			name:     "arrow.DayTimeInterval",
			input:    arrow.DayTimeInterval{Days: 3, Milliseconds: (4*3600 + 5*60 + 6) * 1000},
			expected: "INTERVAL '3 04:05:06' DAY TO SECOND",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeValue(tt.input)
			assert.Equal(t, tt.expected, got)

			// Ensure it can be successfully marshaled to JSON without error
			_, err := json.Marshal(got)
			assert.NoError(t, err, "JSON marshal failed for sanitized value")
		})
	}
}

func TestStatementOutputJSONMarshallingWithMap(t *testing.T) {
	// Simulate statement output containing map[interface{}]interface{}
	rawData := map[interface{}]interface{}{
		"id":    1,
		"name":  "test",
		"props": map[interface{}]interface{}{"color": "red", 123: "numeric_key"},
	}

	sanitizedData := SanitizeValue(rawData)

	queryResult := &session.QueryResult{
		Schema: session.Schema{
			Type: "struct",
			Fields: []session.SchemaField{
				{Name: "col", Type: detectSparkType(rawData), Nullable: true},
			},
		},
		Data: [][]interface{}{
			{sanitizedData},
		},
	}

	stmt := &session.Statement{
		ID:    1,
		Code:  "SELECT * FROM spark_all_datatypes",
		State: session.StatementAvailable,
		Output: &session.StatementOutput{
			Status:         "ok",
			ExecutionCount: 1,
			Data: map[string]interface{}{
				"application/json": queryResult,
			},
		},
		Started:   time.Now().UnixMilli(),
		Completed: time.Now().UnixMilli(),
	}

	bytes, err := json.Marshal(stmt)
	assert.NoError(t, err)
	assert.Contains(t, string(bytes), `"numeric_key"`)
}

func TestArrowTableToQueryResult(t *testing.T) {
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col_int", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "col_str", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "col_month", Type: arrow.FixedWidthTypes.MonthInterval, Nullable: true},
	}, nil)

	intBldr := array.NewInt32Builder(pool)
	defer intBldr.Release()
	intBldr.Append(42)
	intBldr.AppendNull()
	intArr := intBldr.NewInt32Array()
	defer intArr.Release()

	strBldr := array.NewStringBuilder(pool)
	defer strBldr.Release()
	strBldr.Append("hello")
	strBldr.AppendNull()
	strArr := strBldr.NewStringArray()
	defer strArr.Release()

	monthBldr := array.NewMonthIntervalBuilder(pool)
	defer monthBldr.Release()
	monthBldr.Append(arrow.MonthInterval(24))
	monthBldr.AppendNull()
	monthArr := monthBldr.NewMonthIntervalArray()
	defer monthArr.Release()

	cols := []arrow.Column{
		*arrow.NewColumn(schema.Field(0), arrow.NewChunked(schema.Field(0).Type, []arrow.Array{intArr})),
		*arrow.NewColumn(schema.Field(1), arrow.NewChunked(schema.Field(1).Type, []arrow.Array{strArr})),
		*arrow.NewColumn(schema.Field(2), arrow.NewChunked(schema.Field(2).Type, []arrow.Array{monthArr})),
	}
	for i := range cols {
		defer cols[i].Release()
	}

	tbl := array.NewTable(schema, cols, 2)
	defer tbl.Release()

	res, err := ArrowTableToQueryResult(tbl)
	assert.NoError(t, err)
	assert.NotNil(t, res)

	schemaStruct, ok := res.Schema.(session.Schema)
	assert.True(t, ok)
	assert.Len(t, schemaStruct.Fields, 3)
	assert.Equal(t, "integer", schemaStruct.Fields[0].Type)
	assert.Equal(t, "string", schemaStruct.Fields[1].Type)
	assert.Equal(t, "interval", schemaStruct.Fields[2].Type)

	assert.Len(t, res.Data, 2)
	// Row 0
	assert.Equal(t, int32(42), res.Data[0][0])
	assert.Equal(t, "hello", res.Data[0][1])
	assert.Equal(t, "INTERVAL '2-0' YEAR TO MONTH", res.Data[0][2])

	// Row 1 (all nulls)
	assert.Nil(t, res.Data[1][0])
	assert.Nil(t, res.Data[1][1])
	assert.Nil(t, res.Data[1][2])

	// Verify json serialization
	jsonBytes, err := json.Marshal(res)
	assert.NoError(t, err)
	assert.Contains(t, string(jsonBytes), "INTERVAL '2-0' YEAR TO MONTH")
}

func TestEmptyArrowTableToQueryResult(t *testing.T) {
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col_int", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "col_month", Type: arrow.FixedWidthTypes.MonthInterval, Nullable: true},
	}, nil)

	intBldr := array.NewInt32Builder(pool)
	defer intBldr.Release()
	intArr := intBldr.NewInt32Array()
	defer intArr.Release()

	monthBldr := array.NewMonthIntervalBuilder(pool)
	defer monthBldr.Release()
	monthArr := monthBldr.NewMonthIntervalArray()
	defer monthArr.Release()

	cols := []arrow.Column{
		*arrow.NewColumn(schema.Field(0), arrow.NewChunked(schema.Field(0).Type, []arrow.Array{intArr})),
		*arrow.NewColumn(schema.Field(1), arrow.NewChunked(schema.Field(1).Type, []arrow.Array{monthArr})),
	}
	for i := range cols {
		defer cols[i].Release()
	}

	tbl := array.NewTable(schema, cols, 0)
	defer tbl.Release()

	res, err := ArrowTableToQueryResult(tbl)
	assert.NoError(t, err)
	assert.NotNil(t, res)

	schemaStruct, ok := res.Schema.(session.Schema)
	assert.True(t, ok)
	assert.Len(t, schemaStruct.Fields, 2)
	assert.Equal(t, "integer", schemaStruct.Fields[0].Type)
	assert.Equal(t, "interval", schemaStruct.Fields[1].Type)
	assert.Empty(t, res.Data)
}

func TestParseSparkDuration(t *testing.T) {
	tests := []struct {
		input       string
		expected    time.Duration
		expectError bool
	}{
		{"120m", 2 * time.Hour, false},
		{"60m", 1 * time.Hour, false},
		{"2h", 2 * time.Hour, false},
		{"30s", 30 * time.Second, false},
		{"500ms", 500 * time.Millisecond, false},
		{"1d", 24 * time.Hour, false},
		{"2.5d", 60 * time.Hour, false},
		{"-1", -1 * time.Nanosecond, false},
		{"-1ms", -1 * time.Nanosecond, false},
		{"-1s", -1 * time.Nanosecond, false},
		{"-1m", -1 * time.Nanosecond, false},
		{"60000", 60 * time.Second, false},
		{"", 0, true},
		{"invalid-duration", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			d, err := ParseSparkDuration(tc.input)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, d)
			}
		})
	}
}

