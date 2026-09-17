# Livy-Next v1.0.0: Next-Gen Apache Spark 4.x Connect Gateway 🚀

We are proud to announce the milestone release of **Livy-Next v1.0.0** — a high-performance, cloud-native REST gateway for **Apache Spark 4.x Connect** written in Go.

`livy-next` bridges legacy interactive data platforms (Apache Airflow, Jupyter notebooks, custom analytics portals, BI tools) with modern Spark 4.x deployments by translating Livy-compatible REST requests into high-velocity gRPC calls over the **Spark Connect** protocol.

---

## 🌟 Release Highlights

### ⚡ Sub-Millisecond Dispatch & Zero Local JVM
Traditional Apache Livy required a heavy local JVM Spark driver process per session. `livy-next` replaces this with a compiled, single-binary Go gateway that interacts with Spark Connect remote endpoints (`sc://`) over gRPC, reducing gateway memory footprints from gigabytes to megabytes.

### 🔍 Dynamic Spark 4.x Runtime Probing
Livy-Next now dynamically probes the remote Spark Connect server on boot and on every new session:
- Queries the exact Spark runtime version via `SELECT version()`.
- Queries the active cluster master URL via `spark.master`.
- Exposes cluster metadata via `GET /version` and `GET /sessions`.
- Dynamically renders cluster version badges and deep-links in the dashboard UI.

### ⏱️ Spark Connect Idle Timeout Synchronization
Eliminates session lifecycle desynchronization between Livy and Spark Connect:
- Automatically queries and inherits `spark.connect.session.timeout` from the remote server.
- Session reapers remain in lockstep with the cluster, preventing orphaned server-side resources or premature gateway session evictions.

### 🆔 Dual Session Addressing (Numeric Index & UUID)
Seamlessly interoperate between legacy sequential session IDs and canonical Spark Connect UUIDs:
- Query, submit statements, or cancel sessions via sequential integer index (`/sessions/0`) or standard UUID (`/sessions/bcc26fc0-6c1f-4967-8e9a-06364309cb30`).
- Updated OpenAPI / Swagger 2.0 specifications allow string UUID paths without schema validation errors.

### 🖥️ Modern Fixed-Viewport Dashboard UI
Complete architectural overhaul of the embedded `/ui` dashboard:
- **Locked 100vh Viewport**: The outer document layout remains pinned with zero browser scrollbars.
- **Dedicated Inner Scrolling**: Independent scrolling containers for the SQL Query Editor (`max-height: 190px; resize: vertical;`), Tabular Results (`.results-table-scroll` with sticky headers and pagination footer), Live Logs (`.terminal-box`), Session Lists, and Cluster Configuration.
- **Custom Dark Scrollbars**: Sleek 7px scrollbar aesthetic across all modern browsers.

### 🛡️ Graceful Shutdown & Lifecycle Management
- Traps POSIX `SIGTERM` and `SIGINT` signals across both the Go service and container entrypoints.
- Drains active HTTP requests gracefully while transmitting `ReleaseSession` RPCs to the remote Spark Connect cluster before exit.

### 🎨 Brand Identity & SEO Specifications
- Standalone vector emblem (`assets/logo.svg`) and horizontal header banner (`assets/logo-banner.svg`).
- Embedded SVG favicon and `/favicon.ico` HTTP route.
- Complete Open Graph, Twitter Cards, PWA mobile web app, and SEO meta tags embedded in `<head>`.

---

## 📦 Quickstart

### Binary Execution
```bash
./bin/livy-next \
  --addr :8998 \
  --spark-remote sc://localhost:15002 \
  --spark-ui-url http://localhost:4141 \
  --cors-allowed-origins "*"
```

### Docker / Containerized Cluster
```bash
# Verify health and cluster runtime
curl http://localhost:8998/version

# Create interactive session
curl -X POST http://localhost:8998/sessions \
  -H "Content-Type: application/json" \
  -d '{"name": "analytics-session", "kind": "spark"}'

# Submit SQL statement
curl -X POST http://localhost:8998/sessions/0/statements \
  -H "Content-Type: application/json" \
  -d '{"code": "SELECT 42 AS answer, current_timestamp() AS ts"}'

# Access Embedded UI
open http://localhost:8998/ui

# Access Swagger API Playground
open http://localhost:8998/swagger/index.html
```

---

## 🛠️ Detailed Commits in this Release

- `cfdff08`: enhance Spark Connect session management, identity decoupling, and result pagination
- `6f4a46b`: default spark-ui-url to port 4141 and add dynamic client-side URL remapping
- `58d79ae`: enrich OpenAPI spec annotations, request/response models, and examples
- `f47bcad`: implement graceful shutdown signal handling and disconnect detection
- `4782a08`: support both numeric ID and UUID string session identifiers in API routes and Swagger UI
- `c2e35e2`: dynamically inherit and synchronize idle timeout from Spark Connect server
- `8a81ce4`: modernize livy-next embedded UI for Spark 4.x and Spark Connect
- `365105f`: dynamically query exact spark runtime version and master info from connect server
- `5b2fc8f`: fix scrolling by removing overflow:hidden lock, adding min-height:0 to flex containers
- `7f7dd05`: design custom Livy-Next logo, SVG icons, favicon, README banner, and SEO meta tags
- `585bb63`: lock main viewport and enable independent inner scrolling for results, sql editor, logs, and session list

---

## 👥 Contributors & Feedback
Thank you to everyone testing and contributing to the **Livy-Next** project! Please report any feedback or feature requests on the GitHub Issues tracker.
