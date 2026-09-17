# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.0] - 2026-09-17

### Added
- **Dynamic Spark 4.x Runtime Probing**:
  - Boot-time asynchronous cluster probe discovering exact Spark version (`SELECT version()`) and master URL (`spark.master`).
  - Runtime cluster information populated in `GET /version` and `GET /sessions`.
  - Per-session runtime inspection in both REST responses and dashboard UI.
- **Spark Connect Idle Timeout Synchronization**:
  - Automatic query and inheritance of `spark.connect.session.timeout` directly from the Spark Connect server.
  - Two-way session lifecycle synchronization preventing desynchronization between Livy-Next and Spark Connect.
- **Dual Session Addressing**:
  - Support for both sequential zero-based integer IDs (`/sessions/0`) and standard UUIDs (`/sessions/bcc26fc0-...`).
  - Seamless route resolution across all session and statement endpoints.
  - Full OpenAPI / Swagger 2.0 support for string/UUID parameters without type restriction errors.
- **Graceful Lifecycle Management**:
  - POSIX signal trapping (`SIGTERM`, `SIGINT`) in both the Go runtime and container entrypoint scripts.
  - Orderly drainage of HTTP listeners.
  - Active session teardown sending `ReleaseSession` gRPC RPCs to remote Spark Connect servers before process exit.
- **Modern Fixed-Viewport Dashboard UI**:
  - 100vh app architecture (`html, body { height: 100vh; overflow: hidden; }`) with zero window scrollbars.
  - Dedicated independent inner scrolling for SQL Query Editor, Results Dataframe Table, Live Logs, Session Lists, and Spark Configuration.
  - Sticky table headers (`th`) with solid dark backgrounds preventing rows from bleeding through during vertical scroll.
  - Pinned results pagination bar and client-side page controls.
  - High-contrast custom dark-mode scrollbars.
- **Brand Identity & SEO Meta Specifications**:
  - Custom vector brand emblem (`assets/logo.svg`) and horizontal header banner (`assets/logo-banner.svg`).
  - Embedded SVG favicon (`data:image/svg+xml,...`) and HTTP `/favicon.ico` handler supporting `GET` and `HEAD`.
  - Complete `<meta>` specifications: Open Graph (`og:*`), Twitter Cards, PWA mobile web app capability, and keywords.
- **Multi-Tenant Identity & Deep Linking**:
  - Full decoupling of `user_id`, `session_id`, `user_agent`, and authentication `token`.
  - Direct deep-linking to Spark Connect UI sessions (`/connect/session/?id=<sessionId>`) and Spark History Server (`/history/<appId>`).
- **Interactive OpenAPI / Swagger 2.0 Documentation**:
  - Embedded Swagger UI at `/swagger/index.html`.
  - Rich parameter annotations, request/response models, and error responses.

### Changed
- Migrated default Spark UI proxy port to `:4141` matching modern container configurations.
- Upgraded default statement limit configuration via `--default-statement-limit`.
- Enhanced session reaper routine to preserve stopped/dead sessions in history for configurable retention duration (`--dead-timeout`) before eviction.

### Fixed
- Fixed process termination command collisions by eliminating broad pattern matches (`pkill -f`) in favor of exact binary matching (`pkill -x`).
- Fixed flex item minimum size overflow (`min-height: 0`) in UI tab panels and results tables.
- Fixed session timeout desync where Livy-Next prematurely evicted sessions while Spark Connect remained active.
