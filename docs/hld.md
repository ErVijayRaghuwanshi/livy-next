# High-Level Design (HLD)

This document describes the high-level system architecture, network boundaries, topology, and execution flows for **`livy-next`**.

---

## 1. System Context & Boundaries

`livy-next` is a stateless translation gateway. It is designed to expose the traditional Apache Livy REST API while communicating with modern Spark 4.x clusters via the gRPC-based Spark Connect protocol.

```mermaid
graph LR
    subgraph Clients["REST Clients"]
        A["Jupyter / Sparkmagic"]
        B["Airflow Schedulers"]
        C["Custom App / BI Tools"]
    end

    subgraph Gateway["Livy-Next Gateway (Statically Compiled)"]
        D["HTTP API Engine (Chi Router)"]
        E["Session Registry Manager"]
        F["Spark Connect Client Wrapper"]
        G["Dashboard Dashboard & API Specs (Swagger)"]
    end

    subgraph Target["Target Spark 4.x Environment"]
        H["Spark Connect Server (JVM)"]
        I["Spark Master"]
        J["Spark Workers"]
    end

    A -.->|HTTP REST port 8998| D
    B -.->|HTTP REST port 8998| D
    C -.->|HTTP REST port 8998| D

    E -.->|Thread-safe registry| D
    F -->|gRPC remote port 15002| H
    H -->|Logical Plan Execution| I
    I -->|Distributed Tasks| J
```

---

## 2. request/response Sequence Diagrams

### 2.1 Interactive Session Creation (`POST /sessions`)

When a client requests a new session, the gateway validates the session type, initializes a remote Spark Connect gRPC connection channel, and adds the session metadata to its thread-safe registry.

```mermaid
sequenceDiagram
    autonumber
    actor Client as Legacy Client
    participant Router as Chi Router
    participant Mgr as Session Manager
    participant ClientGen as Client Creator
    participant SparkRemote as Spark Connect Server

    Client->>Router: POST /sessions (JSON: {kind: "spark", name: "my-session", conf: {...}, jars: [...]})
    Router->>ClientGen: Creator(kind, conf, jars)
    ClientGen->>SparkRemote: Dial gRPC Remote (sc://localhost:15002)
    SparkRemote-->>ClientGen: gRPC Channel established
    ClientGen->>SparkRemote: SetConfig(key, value) for each config
    ClientGen->>SparkRemote: ExecuteSQL("ADD JAR <jar>") for each jar
    ClientGen-->>Router: *SparkClient (connected & configured)
    Router->>Mgr: CreateSession(name, kind, SparkClient)
    Note over Mgr: Spawns Session metadata & idle-timeout checker
    Mgr-->>Router: *Session (State: starting)
    Router->>Router: Transition State to "idle"
    Router->>SparkRemote: GetAppID (Query spark.app.id)
    SparkRemote-->>Router: app-20260711072356-0000
    Router->>Mgr: SetAppInfo(appId, sparkUiUrl)
    Router-->>Client: 201 Created (JSON Session details including appId & sparkUiUrl)
```

---

### 2.2 Asynchronous Statement Submission & Polling

Livy uses an asynchronous statement execution model. `livy-next` handles this by immediately returning a `waiting` state, executing the SQL query on a background Go routine, and holding the result in memory for client polling.

```mermaid
sequenceDiagram
    autonumber
    actor Client as Legacy Client
    participant Router as Chi Router
    participant Mgr as Session Manager
    participant Session as Session Instance
    participant Spark as Spark Connect Client
    participant SparkRemote as Spark Connect Server

    Client->>Router: POST /sessions/0/statements (JSON: {code: "SELECT 1 + 1"})
    Router->>Mgr: GetSession(0)
    Mgr-->>Router: *Session
    Router->>Session: SubmitStatement(code)
    Note over Session: Creates *Statement (waiting)<br/>Updates Session State to "busy"
    Session-->>Router: *Statement (waiting)
    Router-->>Client: 201 Created (JSON Statement details)

    Note over Session: Spawns runStatement() Goroutine
    activate Session
    Session->>Session: Transition statement to "running"
    Session->>Spark: ExecuteSQL(ctx, query)
    activate Spark
    Spark->>SparkRemote: ExecutePlanRequest (SQL Relation)
    activate SparkRemote
    SparkRemote-->>Spark: Stream ExecutePlanResponse (Arrow RecordBatches)
    deactivate SparkRemote
    Spark->>Spark: Parse Arrow RecordBatches into JSON maps & type schema
    Spark-->>Session: *QueryResult (data & schema)
    deactivate Spark
    
    Session->>Session: Transition statement to "available"
    Session->>Session: Transition session to "idle"
    deactivate Session

    loop Client Polling
        Client->>Router: GET /sessions/0/statements/0
        Router->>Session: GetStatement(0)
        Session-->>Router: *Statement (available)
        Router-->>Client: 200 OK (JSON Statement output + schema)
    end
```
