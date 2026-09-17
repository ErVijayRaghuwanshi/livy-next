<p align="center">
  <img src="assets/logo-banner.svg" alt="Livy-Next Logo Banner" width="650"/>
</p>

<p align="center">
  <strong>High-Performance Cloud-Native Gateway for Apache Spark 4.x Connect</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Release-v1.0.0-6366f1?style=flat-square&logo=github" alt="Release v1.0.0"/>
  <img src="https://img.shields.io/badge/Apache%20Spark-4.x%20Connect-blue?style=flat-square" alt="Spark 4.x Connect"/>
  <img src="https://img.shields.io/badge/Runtime-Go%201.24-00ADD8?style=flat-square&logo=go" alt="Go"/>
  <img src="https://img.shields.io/badge/Footprint-Zero%20JVM-success?style=flat-square" alt="Zero JVM"/>
  <img src="https://img.shields.io/badge/API-Swagger%202.0-85EA2D?style=flat-square&logo=swagger" alt="Swagger"/>
  <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=flat-square" alt="License"/>
</p>

---

# Livy-Next 🚀

`livy-next` is a modern, lightweight, high-performance Apache Livy successor written in Go. It acts as an interactive REST API gateway for modern **Spark 4.x** clusters by translating traditional Livy-compatible REST requests into high-speed gRPC calls over the **Spark Connect** protocol.

This allows legacy notebook tools, workflow orchestrators (such as Apache Airflow), and microservices to interface with Spark 4.x clusters seamlessly with sub-millisecond dispatch overhead and zero local JVM footprint.

---

## Key Features (v1.0.0)

- **Livy REST API Compatibility**: Drop-in REST compatibility for managing interactive sessions and executing asynchronous statement blocks.
- **Spark Connect Native**: Built on top of the Spark Connect gRPC protocol, communicating directly with Spark Connect remote endpoints (`sc://`) with Apache Arrow columnar data transport.
- **Dynamic Cluster Probing**: Automatically queries exact Spark runtime version (`SELECT version()`) and master URL (`spark.master`) on boot and per-session, exposed via `/version` and `/sessions`.
- **Idle Timeout Synchronization**: Automatically queries and synchronizes session idle timeouts directly with the Spark Connect server configuration (`spark.connect.session.timeout`).
- **Dual Session Addressing**: Seamlessly accepts both sequential numeric IDs (`/sessions/0`) and standard UUIDs (`/sessions/bcc26fc0-...`) across all API routes and the Swagger UI.
- **Modern Dashboard UI**: Rich, fixed-viewport dark-mode dashboard at `/ui` with independent inner scrolling (SQL editor, live logs, session lists, dataframe results with sticky table headers and client pagination).
- **Graceful Lifecycle Management**: Handles OS signals (`SIGTERM`, `SIGINT`) gracefully, draining HTTP connections and releasing all active remote Spark Connect sessions via `ReleaseSession` RPCs before exit.
- **Embedded Swagger UI**: Interactive OpenAPI 2.0 playground and API specifications served under `/swagger/index.html`.
- **Configurable Result Limits & Deep Links**: Supports pagination (`from`, `size`) and custom statement limits (`--default-statement-limit`), plus deep links directly to Spark Connect UI sessions.
- **Mock/Offline Mode**: Full support for running the gateway in mock mode via `--mock` for local CI/CD verification without a live Spark cluster.
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
| `--spark-ui-url` | `http://localhost:4141` | Base URL for the Spark Web UI (deep-linked from sessions). |
| `--spark-history-url` | `http://localhost:18088` | Base URL for the Spark History Server UI. |
| `--spark-token` | `""` | Pre-shared authentication token for Spark Connect. |
| `--idle-timeout` | `30m` | Duration after which running inactive sessions are automatically terminated. |
| `--sync-session-timeout` | `true` | Dynamically inherit and synchronize idle timeout from Spark Connect server. |
| `--dead-timeout` | `5m` | Duration after which dead/stopped sessions are permanently removed from history. |
| `--default-statement-limit` | `10000` | Default maximum rows returned by SQL statements (0 for unlimited). |
| `--grpc-keepalive-time` | `60s` | gRPC keepalive ping frequency for Spark Connect channels. |
| `--grpc-keepalive-timeout` | `20s` | gRPC keepalive ping timeout duration. |
| `--cors-allowed-origins` | `*` | Comma-separated list of allowed CORS origins. |
| `--mock` | `false` | Enable in-memory mock Spark client for offline CI/CD verification. |

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

### 4. Query Runtime Cluster Info
```bash
curl http://localhost:8998/version
```
*Response*:
```json
{
  "version": "1.0.0",
  "sparkVersion": "4.2.0",
  "sparkMaster": "spark://spark-master:7077",
  "build": "livy-next"
}
```

### 5. Dual Session Addressing (Numeric Index or UUID)
Sessions can be accessed interchangeably using their zero-based sequential integer ID or their canonical Spark Connect UUID:
```bash
# Query via numeric index
curl http://localhost:8998/sessions/0

# Query via Spark Connect Session UUID
curl http://localhost:8998/sessions/e98fe459-d482-458e-9671-6784218d4c02
```

### 6. Delete Session (Graceful Teardown)
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
