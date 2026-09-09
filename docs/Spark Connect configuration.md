Yes — **we absolutely should document the server-side session timeout**. I checked the Spark 4.x source, and there is an important correction to our earlier discussion:

> **Spark Connect has a server-side inactive-session timeout, and the default is 60 minutes.**

The configuration is:

```text
spark.connect.session.manager.defaultSessionTimeout
```

It was introduced in Spark 4.0.0, is marked internal, and defaults to **`60m`**. A session is removed after it receives no new incoming RPC for that period. Setting it to `-1` keeps sessions forever. ([GitHub][1])

So I would definitely add a **"Spark Connect Server Configuration"** section to our document.

## Recommended configuration checklist

For your Spark 4.1.2 server, I'd document these categories:

| Area                     | Configuration                                         | Default / recommendation   |
| ------------------------ | ----------------------------------------------------- | -------------------------- |
| **Session lifecycle**    | `spark.connect.session.manager.defaultSessionTimeout` | **60m** default            |
| Session cleanup          | `spark.connect.session.manager.maintenanceInterval`   | **30s**                    |
| UI history               | `spark.sql.connect.ui.retainedSessions`               | 200                        |
| UI statement history     | `spark.sql.connect.ui.retainedStatements`             | 200                        |
| Abandoned executions     | `spark.connect.execute.manager.detachedTimeout`       | **5m**                     |
| Execution cleanup        | `spark.connect.execute.manager.maintenanceInterval`   | **30s**                    |
| Result transport         | `spark.connect.session.resultChunking.maxChunkSize`   | Consider for large results |
| gRPC client message size | `grpc_max_message_size`                               | **128 MB**                 |
| gRPC keepalive           | `grpc_keepalive_*`                                    | 60s / 20s client defaults  |
| Authentication           | `spark.connect.authenticate.token`                    | Optional                   |
| Spark network            | `spark.network.timeout`                               | 120s                       |

The first four are particularly relevant to your Jupyter architecture. Spark's Connect configuration source documents the session timeout, maintenance interval, execution timeout, UI retention, and result chunking settings. ([GitHub][1])

### 1. Session timeout — definitely document this

```text
spark.connect.session.manager.defaultSessionTimeout=60m
```

Meaning:

> Remove a Spark Connect session if there are no new incoming RPCs for 60 minutes.

This is **not** the same as a gRPC connection timeout.

You can change it, for example:

```bash
--conf spark.connect.session.manager.defaultSessionTimeout=2h
```

Or disable expiration:

```bash
--conf spark.connect.session.manager.defaultSessionTimeout=-1
```

I would **not recommend `-1` in a multi-user Jupyter platform** unless you have another explicit session-cleanup mechanism, because abandoned sessions can accumulate.

---

### 2. Session maintenance interval

```text
spark.connect.session.manager.maintenanceInterval=30s
```

The default is 30 seconds.

This controls how frequently the session manager checks for expired sessions. ([GitHub][1])

So if you configure:

```text
defaultSessionTimeout=60m
maintenanceInterval=30s
```

think of it approximately as:

```text
No RPC
  │
  ├──────────── 60 minutes ────────────┐
  │                                    │
  │                              session expires
  │                                    │
  │                              cleanup check
  │                                    │
  └────────────────────────────────────┴──► ~30s
```

The exact cleanup timing is therefore not necessarily exactly 60:00.

---

## 3. Don't confuse session timeout with execution timeout

Spark Connect also has:

```text
spark.connect.execute.manager.detachedTimeout=5m
```

The default is **5 minutes**. This is for executions that no longer have an attached RPC, not idle Spark sessions. ([GitHub][1])

This matters for your Jupyter use case.

For example:

```text
Jupyter
   │
   │ Execute query
   ▼
Spark Connect
   │
   ├── Session still exists
   │
   └── Query execution
           │
           └── client disconnects
                    │
                    ▼
              detached execution
                    │
                    └── cleanup after configured timeout
```

So we should document these separately.

---

# 4. Large-result configuration

This is especially relevant given our pagination discussion.

Spark 4.x has:

```text
spark.connect.session.resultChunking.maxChunkSize
```

The Spark Connect server supports result chunking for large result batches. The feature is enabled when the configuration is greater than zero and the client allows result chunking. ([GitHub][1])

This is something I'd add to our **large-result / Jupyter** section.

Separately, the Spark Connect client connection string has:

```text
grpc_max_message_size
```

whose documented default is:

```text
128 * 1024 * 1024
```

or **128 MB**. ([GitHub][2])

So we have to distinguish:

```text
Spark Connect result chunking
        ↓
How Spark divides result responses

gRPC max message size
        ↓
Maximum individual gRPC message

Pagination
        ↓
How much data the application logically requests

toLocalIterator()
        ↓
How the client consumes the result
```

These are related but different controls.

---

# 5. gRPC keepalive

The client supports:

```text
grpc_keepalive_enabled=true
grpc_keepalive_time_ms=60000
grpc_keepalive_timeout_ms=20000
grpc_keepalive_without_calls=true
```

The current Spark Connect connection-string documentation describes these as mechanisms for detecting silently dead connections, such as a NAT/load balancer dropping an idle mapping. ([GitHub][2])

**They do not control Spark Connect session expiration.**

So:

```text
60 minute session timeout
```

and:

```text
60 second gRPC keepalive
```

are completely different things.

---

# 6. UI retention

Since you've already confirmed that you're using:

```text
http://localhost:4141/connect/
```

I'd also document:

```text
spark.sql.connect.ui.retainedSessions
```

Default:

```text
200
```

This controls the number of client sessions retained in the Spark Connect UI history. ([GitHub][1])

Similarly:

```text
spark.sql.connect.ui.retainedStatements
```

defaults to:

```text
200
```

and controls the number of statements retained in Connect UI history. ([GitHub][1])

These are **UI history limits**, not session lifetime limits.

---

# 7. Authentication

For a production deployment, we should also document authentication.

Spark Connect has:

```text
spark.connect.authenticate.token
```

which can use a pre-shared authentication token. The configuration can also be supplied through:

```text
SPARK_CONNECT_AUTHENTICATE_TOKEN
```

according to the Spark Connect server configuration source. ([GitHub][1])

For your local development environment this may not matter, but for:

```text
JupyterHub
    ↓
Spark Connect
```

it becomes important.

---

# 8. Recommended section to add to our document

I'd add this to the architecture document:

```text
Spark Connect Server Configuration
──────────────────────────────────

1. Network
   - Connect port
   - Binding address
   - TLS

2. Authentication
   - Token authentication
   - Identity/user_id strategy

3. Session lifecycle
   - defaultSessionTimeout = 60m
   - maintenanceInterval = 30s

4. Execution lifecycle
   - detachedTimeout = 5m
   - execution maintenance interval

5. Result transport
   - result chunking
   - gRPC max message size

6. Connection health
   - gRPC keepalive

7. UI monitoring
   - retainedSessions = 200
   - retainedStatements = 200

8. Spark application
   - app name
   - driver configuration
   - executor configuration
   - event logging
```

### And for your particular Jupyter deployment

I'd start with something like:

```text
spark.connect.session.manager.defaultSessionTimeout=60m
spark.connect.session.manager.maintenanceInterval=30s
spark.connect.execute.manager.detachedTimeout=5m
spark.connect.execute.manager.maintenanceInterval=30s
spark.sql.connect.ui.retainedSessions=200
spark.sql.connect.ui.retainedStatements=200
```

Then tune **result chunking and gRPC message size** after we benchmark the 1-million-row test we just created.

One especially useful next step would be to build a **complete `spark-defaults.conf` for your Spark 4.1.2 Connect server**, with comments explaining every setting and separating **production**, **development**, and **Jupyter-large-result** configurations.

[1]: https://github.com/apache/spark/blob/master/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala?utm_source=chatgpt.com "spark/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala at master · apache/spark · GitHub"
[2]: https://github.com/apache/spark/blob/master/sql/connect/docs/client-connection-string.md?utm_source=chatgpt.com "spark/sql/connect/docs/client-connection-string.md at master · apache/spark · GitHub"
