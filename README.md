# Livy-Next 🚀

`livy-next` is a modern, lightweight, high-performance Apache Livy successor written in Go. It acts as an interactive REST API gateway for modern **Spark 4.x** clusters by translating traditional Livy-compatible REST requests into gRPC calls over the **Spark Connect** protocol.

This allows legacy notebook tools, workflow orchestrators (like Apache Airflow), and custom applications to interface with Spark 4.x clusters seamlessly without running heavy local JVM-based Spark driver processes.

---

## Key Features

- **Livy API Compatibility**: Drop-in REST compatibility for managing interactive sessions and executing asynchronous statement blocks.
- **Spark Connect Native**: Built on top of the Spark Connect gRPC protocol, communicating directly with Spark Connect remote endpoints (`sc://`).
- **Interactive Dashboard UI**: Rich, embedded dark-mode dashboard at `/ui` to view active and historical sessions, inspect **Spark Connect Session IDs**, execute SQL queries dynamically, and monitor logs.
- **Mock/Offline Mode**: Full support for running the gateway in mock mode via the `--mock` flag for local verification without a live Spark cluster.
- **Robust Session Lifecycles**: Stopped or terminated sessions transition to a `dead` state, are preserved in history (with statement execution disabled), and are automatically reaped after the configured idle timeout.
- **Graceful Session Release**: Direct server-side gRPC session teardown utilizing the Spark Connect `ReleaseSession` RPC protocol when terminating.
- **Embedded Swagger UI**: Interactive API testing playground and OpenAPI documentation served under `/swagger/index.html`.
- **Configurable CORS**: Full control over cross-origin request policies via simple command-line flags.

---

## Getting Started

### Prerequisites

1. **Go**: Version `1.21` or later.
2. **Spark Connect Server**: A running Spark 4.x cluster with Spark Connect server enabled.
   To start the Spark Connect server locally:
   ```bash
   $SPARK_HOME/sbin/start-connect-server.sh --master local[*]
   ```
   *Note: If you plan to use Delta Lake, start the server specifying the package and extensions:*
   ```bash
   $SPARK_HOME/sbin/start-connect-server.sh \
     --packages io.delta:delta-spark_2.13:4.0.0 \
     --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
   ```

### Building & Running

A `Makefile` is provided to compile, tidy, test, and run the service.

1. **Initialize and download dependencies**:
   ```bash
   make tidy
   ```

2. **Generate Swagger docs & build the binary**:
   ```bash
   make build
   ```

3. **Run the gateway**:
   ```bash
   ./bin/livy-next --addr :8998 --spark-remote sc://localhost:15002 --cors-allowed-origins "*"
   ```

#### Command-Line Options

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--addr` | `:8998` | The host and port address `livy-next` binds to. |
| `--spark-remote` | `sc://localhost:15002` | Connection string for the remote Spark Connect server. |
| `--idle-timeout` | `30m` | Duration after which running inactive sessions are automatically terminated. |
| `--dead-timeout` | `5m` | Duration after which dead/stopped sessions are permanently removed from history. |
| `--cors-allowed-origins` | `*` | Comma-separated list of allowed CORS origins. |
| `--mock` | `false` | Enable in-memory mock Spark client for offline testing. |

---

## API Usage Examples

### 1. Create a Session
```bash
curl -X POST http://localhost:8998/sessions \
  -H "Content-Type: application/json" \
  -d '{"kind": "spark", "name": "my-session"}'
```
*Response*:
```json
{
  "id": 0,
  "name": "my-session",
  "state": "idle",
  "kind": "spark",
  "appInfo": {},
  "log": ["Session created"],
  "statements": [],
  "lastActivity": "2026-07-11T00:30:00Z"
}
```

### 2. Submit SQL Statement
```bash
curl -X POST http://localhost:8998/sessions/0/statements \
  -H "Content-Type: application/json" \
  -d '{"code": "SELECT 100 * 200 AS multiplication_test"}'
```
*Response*:
```json
{
  "id": 0,
  "code": "SELECT 100 * 200 AS multiplication_test",
  "state": "waiting",
  "progress": 0
}
```

### 3. Poll Statement Execution Result
```bash
curl http://localhost:8998/sessions/0/statements/0
```
*Response*:
```json
{
  "id": 0,
  "code": "SELECT 100 * 200 AS multiplication_test",
  "state": "available",
  "output": {
    "status": "ok",
    "execution_count": 0,
    "data": {
      "application/json": {
        "schema": {
          "type": "struct",
          "fields": [
            {
              "name": "multiplication_test",
              "type": "integer",
              "nullable": true,
              "metadata": {}
            }
          ]
        },
        "data": [
          [20000]
        ]
      }
    }
  },
  "progress": 1
}
```

### 4. Delete Session
```bash
curl -X DELETE http://localhost:8998/sessions/0
```

---

## Project Structure

```
livy-next/
├── cmd/
│   └── livy-next/
│       └── main.go         # Entry point: parses flags and starts web server
├── pkg/
│   ├── api/
│   │   ├── handler.go      # REST HTTP handlers matching Livy API paths
│   │   ├── router.go       # Chi router definitions & CORS middleware
│   │   └── ui/
│   │       └── index.html  # Embedded Dark Mode Dashboard HTML/JS
│   ├── session/
│   │   ├── manager.go      # Global session manager and reaper
│   │   └── session.go      # Session model & asynchronous worker loop
│   └── spark/
│       └── client.go       # Spark Connect Client and Result Parser
├── docs/                   # Swagger OpenAPI static specifications
├── Makefile                # Lifecycle management scripts
└── go.mod                  # Go module definition
```

---

## License

This project is licensed under the Apache License 2.0. See the LICENSE file for details.
