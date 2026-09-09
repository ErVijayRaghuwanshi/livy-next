# Project Roadmap

This document outlines the development phases, milestones, and future roadmap for **`livy-next`**.

---

## Roadmap Overview

```mermaid
gantt
    title Development Timeline
    dateFormat  YYYY-MM
    section Phase 1 (Core REST & SQL)
    Chi REST API Gateway       :done, des1, 2026-06, 2026-07
    Spark Connect Client Integration :done, des2, 2026-06, 2026-07
    Glassmorphism Dashboard UI   :done, des3, 2026-07, 2026-07
    section Phase 2 (Connect Parity)
    Forwarding Cancel RPCs to gRPC :active, des4, 2026-07, 2026-08
    Dynamic Configuration Overrides :des5, 2026-08, 2026-09
    Multi-tenant Proxy-User Isolation :des6, 2026-08, 2026-09
    section Phase 3 (REPL & Languages)
    Local PySpark REPL Subprocess Engine :des7, 2026-09, 2026-11
    Local Scala REPL Subprocess Engine   :des8, 2026-10, 2026-12
    section Phase 4 (Enterprise Hardening)
    Redis Session Storage (HA)     :des9, 2026-12, 2027-02
    LDAP / Kerberos / JWT Auth     :des10, 2026-12, 2027-02
    Prometheus Metrics Exporter     :des11, 2027-02, 2027-03
```

---

## Detailed Phases

### Phase 1: Core API & Spark Connect Integration (Completed)
- [x] Lightweight HTTP gateway in Go using `go-chi`.
- [x] Initial Spark Connect gRPC connection capabilities.
- [x] Asynchronous statement running logic utilizing Go routines.
- [x] Thread-safe interactive session registry with idle timeout garbage collection.
- [x] Type translation formatting matching legacy Apache Livy JSON schemas.
- [x] Embedding Web UI console dashboard and Swagger documentation router.
- [x] Configurable CORS origin headers.

---

### Phase 2: Spark Connect Parity & Session Isolation (Completed)
- [x] **Active Cancel Forwarding**: Forward statement cancellations directly via context cancellation to terminate active execution on remote Spark Connect server.
- [x] **Dynamic Configuration Overrides**: Support passing specific cluster configs and custom jar packages in `POST /sessions`, applying configurations dynamically.
- [x] **Proxy-User / Multi-tenant Identity Isolation**: Full decoupling of `user_id`, `session_id` (UUID), `user_agent`, and authentication tokens (`token`), mapping directly to Spark Connect remote connection specifications.
- [x] **Result Pagination & Persistence**: Support standard Livy `from` and `size` parameters on statements, preserving query outputs across multiple reads and preventing premature data purging.
- [x] **Dedicated Spark Connect UI Deep-linking**: Live links in session metadata and web dashboard to `/connect/session/?id=<sessionId>`.

---

### Phase 3: Language REPL Execution Support (Medium-term)
Spark Connect handles DataFrame plan execution but does not natively interpret raw python/scala code blocks. To support classic Livy notebook workflows (`kind: pyspark` and `kind: spark` containing Scala):
- **Local REPL Execution Engine**: Spin up local Python/Scala subprocess interpreters inside the `livy-next` container.
- **Spark Connect Injector**: Inject pre-configured `spark` SparkSession instances into the local python/scala execution contexts that point back to the Spark Connect remote server.
- **Standard Stream Capturer**: Capture stdout/stderr streams from the local subprocess, translating them back to standard Livy JSON output structures (`"text/plain"`).

---

### Phase 4: Enterprise Production Hardening (Long-term)
- **Stateless Gateway (High Availability)**: Move session registry metadata out of in-memory maps and into distributed stores like Redis or PostgreSQL. This allows scaling the `livy-next` HTTP gateway horizontally behind a load balancer.
- **Enterprise Security**:
  - Integrate token-based JWT authentication, Kerberos authentication delegation, or LDAP directory authentication.
  - Implement secure SSL/TLS communication configurations on REST routes and gRPC channels.
- **Observability**:
  - Add standard Prometheus metrics endpoints tracking active session counts, execution latency, error rates, and remote Spark gRPC query performance.
  - Integrate OpenTelemetry for structured trace propagation.
