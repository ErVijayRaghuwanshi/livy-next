# Low-Level Design (LLD)

This document describes the low-level component classes, internal structs, state transition rules, and package-level details for **`livy-next`**.

---

## 1. Code Modules & Struct Layout

`livy-next` is composed of three internal Go packages: `api`, `session`, and `spark`.

```mermaid
classDiagram
    direction TB
    class Manager {
        -sessions: map[int]*Session
        -idleTimeout: time.Duration
        -mu: sync.RWMutex
        +CreateSession(name: string, kind: string, client: SparkClient) *Session
        +GetSession(id: int) (*Session, bool)
        +ListSessions() []*Session
        +DeleteSession(id: int) error
        -startReaper()
    }

    class Session {
        +ID: int
        +Name: string
        +State: SessionState
        +Kind: string
        +AppID: string
        +AppInfo: map[string]string
        +Statements: []*Statement
        +LastActivity: time.Time
        -client: SparkClient
        -mu: sync.RWMutex
        -closeCh: chan
        +SubmitStatement(code: string) *Statement
        +CancelStatement(id: int) bool
        +SetAppInfo(appID: string, uiURL: string)
        -runStatement(stmt: *Statement)
    }

    class Statement {
        +ID: int
        +Code: string
        +State: StatementState
        +Progress: float64
        +Output: *StatementOutput
        +Started: int64
        +Completed: int64
    }

    class SparkClient {
        <<interface>>
        +ExecuteSQL(ctx: context.Context, sql: string) (*QueryResult, error)
        +GetAppID(ctx: context.Context) (string, error)
        +SetConfig(ctx: context.Context, key: string, value: string) error
        +Close() error
    }

    class Client {
        -session: sql.SparkSession
        +ExecuteSQL(ctx: context.Context, sql: string) (*QueryResult, error)
        +GetAppID(ctx: context.Context) (string, error)
        +SetConfig(ctx: context.Context, key: string, value: string) error
        +Close() error
    }

    Manager "1" *-- "many" Session
    Session "1" *-- "many" Statement
    Session ..> SparkClient : uses
    Client ..|> SparkClient : implements
```

---

## 2. State Machine Transition Logic

### 2.1 Session States

A session lifecycle follows the transitions managed by user actions and automated timeout collectors.

```mermaid
stateDiagram-v2
    [*] --> starting : POST /sessions
    starting --> idle : Handler registers client
    idle --> busy : SubmitStatement() triggered
    busy --> idle : All statements complete
    idle --> dead : --idle-timeout threshold reached (Reaper)
    busy --> dead : DELETE /sessions
    idle --> dead : DELETE /sessions
    dead --> [*] : Deleted from Registry
```

### 2.2 Statement States

Individual code blocks follow a linear sequence, transitioning on goroutine status or cancellation.

```mermaid
stateDiagram-v2
    [*] --> waiting : SubmitStatement()
    waiting --> running : runStatement() worker acquires lock
    running --> available : SQL returns data & schema mapped
    running --> error : SQL execution returns error
    waiting --> cancelled : CancelStatement()
    running --> cancelled : CancelStatement()
    available --> [*]
    error --> [*]
    cancelled --> [*]
```

---

## 3. Concurrency & Mutex Safety Patterns

To prevent race conditions, the registry and sessions utilize granular Read/Write Mutexes (`sync.RWMutex`):

1. **Registry Operations**: `session.Manager` locks its internal sessions map with `mu.Lock()` during creation or termination, and utilizes `mu.RLock()` for retrievals and lists.
2. **Session Properties**: Access to state, logs, and statements list is protected by a local `sync.RWMutex` inside each `Session` instance.
3. **Background Statement Worker**: The background thread executing `client.ExecuteSQL` does not hold the session lock during network operations, allowing the REST handlers to query status or request cancellation concurrently.

---

## 4. Spark Connect Type Translation

Spark Connect streams record batches using the Apache Arrow IPC layout. The `spark.Client` maps Arrow column structures into Livy JSON definitions:

| Spark Type Class | Arrow/Go Type | Livy Schema Type |
| :--- | :--- | :--- |
| `IntegerType`, `LongType` | `int32`, `int64` | `"integer"` |
| `DoubleType`, `FloatType` | `float32`, `float64` | `"double"` |
| `BooleanType` | `bool` | `"boolean"` |
| `StringType` | `string` | `"string"` |
| Other Types | Unsupported (Default Fallback) | `"string"` |
