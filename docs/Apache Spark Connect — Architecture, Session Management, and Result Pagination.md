# Apache Spark Connect
## Architecture, Session Management, Lifecycle, and Result Pagination

**Environment**

- PySpark client: **4.2.0**
- Spark Connect server: **Apache Spark 4.1.2**
- Spark Connect endpoint: `sc://localhost:15002`
- Spark application UI: `http://localhost:4141/`
- Spark Connect UI: `http://localhost:4141/connect/`
- Deployment observed:
  - Spark Master: `spark://spark-master:7077`
  - Deploy mode: `client`
  - Spark Connect server runs inside a long-lived Spark application

---

# 1. Spark Connect Architecture and Lifecycle

## 1.1 What Spark Connect is

Spark Connect is a client-server architecture that separates the Python application from the Spark driver/server.

Instead of the traditional PySpark model:

```text
Jupyter
   │
   └── PySpark
        │
        └── Spark Driver
             │
             └── Executors
```

Spark Connect uses:

```text
Jupyter / Python
      │
      │ gRPC
      ▼
Spark Connect Server
      │
      │
      ▼
Spark Driver
      │
      ├── Executor 1
      ├── Executor 2
      └── Executor N
```

The Python process is therefore a remote client. It does not own the Spark driver.

Apache Spark describes Spark Connect as a client-server architecture that decouples Spark client applications from Spark clusters. 

---

## 1.2 Creating a Spark Connect session

The standard Python entry point is:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .remote("sc://localhost:15002")
    .getOrCreate()
)
```

The connection uses gRPC.

The Spark Connect connection URI has the general form:

```text
sc://host:port/;parameter=value;parameter=value
```

For example:

```text
sc://localhost:15002/;user_id=ervijay
```

Spark's connection-string specification defines parameters such as:

- `user_id`
- `session_id`
- `user_agent`
- gRPC keepalive settings
- maximum gRPC message size

---

# 2. Spark Application vs Spark Connect Session

This is one of the most important architectural distinctions.

A **Spark application** and a **Spark Connect session** are different objects.

For example, our deployment has:

```text
Spark Application
app-20260906064013-0000
        │
        └── Spark Connect Server
              │
              ├── Session A
              │    user = ervijay
              │    session = 8caa...
              │
              ├── Session B
              │    user = ervijay
              │    session = d95f...
              │
              └── Other sessions
```

We verified this directly in the Spark 4.1.2 environment:

```text
Session A
8caa8599-ffe9-46ee-bbfd-574c25520ca8
```

and:

```text
Session B
d95f2c41-cca7-4a00-a94a-bb76564fae40
```

both belonged to:

```text
app-20260906064013-0000
```

Therefore:

> Multiple Spark Connect sessions can exist under the same Spark application.

This is particularly important when designing a multi-user Jupyter environment.

---

# 3. Spark Connect Session Identity

A Spark Connect session has two important identity components:

```text
user_id
session_id
```

Spark's connection-string specification states that the Connect server uses the session ID as part of the session cache key, together with the user identity.

Example:

```text
sc://localhost:15002/;
    user_id=ervijay;
    session_id=550e8400-e29b-41d4-a716-446655440000
```

The `session_id` must be a valid UUID.

If it isn't supplied, the Python client generates a random UUID.

Therefore:

```python
spark = (
    SparkSession.builder
    .remote(
        "sc://localhost:15002/"
        ";user_id=ervijay"
    )
    .getOrCreate()
)
```

creates a new Connect session identity using:

```text
user_id   = ervijay
session_id = generated UUID
```

---

# 4. Setting the Username

The Spark Connect username should be supplied using the connection string:

```python
spark = (
    SparkSession.builder
    .remote(
        "sc://localhost:15002/"
        ";user_id=ervijay"
    )
    .getOrCreate()
)
```

This is preferable to attempting:

```python
SparkSession.builder.userId("ervijay")
```

because `Builder.userId()` is not available in the PySpark client API we tested.

The Spark Connect connection specification explicitly defines `user_id`.

The Python Spark Connect implementation also accepts a `userId` when constructing a `SparkSession`, but the connection-string `user_id` takes precedence.

---

# 5. User Agent

Spark Connect also supports a separate `user_agent`:

```python
spark = (
    SparkSession.builder
    .remote(
        "sc://localhost:15002/"
        ";user_id=ervijay"
        ";user_agent=jupyter"
    )
    .getOrCreate()
)
```

This is useful for identifying the client/application acting on behalf of the user.

A useful convention is:

```text
user_id     = ervijay
user_agent  = jupyter
session_id  = UUID
```

This separates the actual user from the application/client they are using.

---

# 6. Application Name

Spark application name is different from Connect session identity.

For example:

```python
spark = (
    SparkSession.builder
    .remote(
        "sc://localhost:15002/"
        ";user_id=ervijay"
    )
    .appName("My Analytics Session")
    .getOrCreate()
)
```

This results in:

```text
spark.app.name = My Analytics Session
```

We verified this directly using:

```python
spark.conf.getAll()
```

which returned:

```text
'spark.app.name': 'My Analytics Session'
```

The same configuration also showed:

```text
'spark.app.id': 'app-20260906064013-0000'
```

Therefore `.appName()` is working correctly.

However:

> `appName` is application-level metadata, not Connect-session metadata.

The Connect session table does not have an `App Name` column.

The application name can instead be found under the application-level Spark UI, particularly the Environment/Spark Properties information.

---

# 7. Connect UI

Spark 4.1.2 provides a dedicated Connect section in the Spark UI.

```text
http://localhost:4141/connect/
```

The Connect page displays information such as:

```text
User
Session ID
Start Time
Finish Time
Duration
Total Execute
```

For example:

```text
User       Session ID                         Duration
ervijay    8caa8599-ffe9-46ee-bbfd-574c...    5m23s
ervijay    d95f2c41-cca7-4a00-a94a-bb765...   5m12s
```

This is the appropriate UI for monitoring Connect sessions.

Individual session details can also be reached using:

```text
/connect/session/?id=<session_id>
```

---

# 8. Spark Application Metadata vs Connect Metadata

The metadata should be conceptually separated:

```text
Spark Application
────────────────────────────
app_id
app_name
spark configurations
driver information
executor information


Spark Connect Session
────────────────────────────
user_id
session_id
user_agent
start time
finish time
execution information


Operation
────────────────────────────
operation ID
operation tags
query execution
job/stage information
```

This distinction prevents application-level properties from being confused with session-level properties.

---

# 9. Custom Metadata

Spark configuration can be used for additional application/session configuration:

```python
spark = (
    SparkSession.builder
    .remote(
        "sc://localhost:15002/"
        ";user_id=ervijay"
    )
    .appName("Customer Analytics")
    .config("my.session.project", "customer-analytics")
    .config("my.session.environment", "production")
    .config("my.session.owner", "ervijay")
    .getOrCreate()
)
```

Then:

```python
spark.conf.get("my.session.project")
```

returns:

```text
customer-analytics
```

However, these custom configurations should not be considered fields of the Connect UI's session table.

They are Spark configuration/session state.

---

# 10. Operation Tags

Spark Connect also supports operation tags.

Example:

```python
spark.addTag("project:customer-analytics")
spark.addTag("notebook:customer-analysis")
spark.addTag("team:data-platform")
```

Tags can be inspected using:

```python
spark.getTags()
```

and removed with:

```python
spark.removeTag("project:customer-analytics")
```

or:

```python
spark.clearTags()
```

Tags are best suited for correlating operations rather than defining the primary Connect session identity.

A useful hierarchy is:

```text
User
  │
  └── Connect Session
        │
        ├── session_id
        ├── user_agent
        │
        └── Operations
              ├── tag: project
              ├── tag: notebook
              └── tag: request
```

---

# 11. Session Lifecycle

A simplified lifecycle is:

```text
Client starts
     │
     ▼
Create Spark Connect session
     │
     ▼
Session registered on Connect server
     │
     ▼
Execute requests
     │
     ├── SQL
     ├── DataFrame operations
     ├── actions
     └── metadata/tags
     │
     ▼
Client calls spark.stop()
     │
     ▼
Connect session released
     │
     ▼
gRPC connection closed
```

The important point is that:

```python
spark.stop()
```

does **not** mean:

```text
Stop Spark application
Stop Spark Connect server
Stop other users
Stop other sessions
```

For remote Spark Connect, `stop()` releases the current Connect session and closes the client's gRPC connection.

The Spark Connect Python implementation explicitly notes that the server is designed for multi-tenancy and that stopping one remote session must not stop the server or other remote clients.

---

# 12. Creating a New Session

A new session can be created with a new session ID.

For example:

```python
import uuid

session_id = str(uuid.uuid4())

spark = (
    SparkSession.builder
    .remote(
        f"sc://localhost:15002/"
        f";user_id=ervijay"
        f";session_id={session_id}"
        f";user_agent=jupyter"
    )
    .appName("Customer Analytics")
    .getOrCreate()
)
```

The resulting identity is:

```text
user_id       = ervijay
session_id    = <generated UUID>
user_agent    = jupyter
app_name      = Customer Analytics
```

---

# 13. Sharing a Session

Because the connection string accepts an explicit `session_id`, clients can intentionally use the same:

```text
user_id
session_id
```

combination.

This is useful when multiple clients/languages need to share the same Spark session.

Example:

```text
Client A
Python
    │
    ├── user_id = ervijay
    └── session_id = ABC


Client B
Another client
    │
    ├── user_id = ervijay
    └── session_id = ABC
```

Both can refer to the same Connect session, subject to the deployment and client behavior.

---

# 14. Session Timeout vs gRPC Keepalive

These should not be confused.

Spark Connect supports gRPC keepalive parameters such as:

```text
grpc_keepalive_enabled
grpc_keepalive_time_ms
grpc_keepalive_timeout_ms
grpc_keepalive_without_calls
```

Example:

```text
sc://localhost:15002/;
grpc_keepalive_time_ms=30000;
grpc_keepalive_timeout_ms=10000
```

These parameters are for detecting dead network connections.

They are **not equivalent to an idle Spark Connect session timeout**.

Conceptually:

```text
gRPC keepalive
    │
    └── Is the network connection still alive?


Connect session lifecycle
    │
    └── Does this Spark Connect session still exist?


Spark application lifecycle
    │
    └── Is the Spark application/driver still running?
```

These are three different lifecycle mechanisms.

---

# 15. Result Pagination

Spark 4.2.0 provides:

```python
DataFrame.offset()
```

which returns a DataFrame after skipping the specified number of rows.

It can therefore be combined with:

```python
orderBy()
limit()
```

to implement pagination.

Example:

```python
page_size = 1000

page = (
    df
    .orderBy("id")
    .offset(0)
    .limit(page_size)
)
```

Page 2:

```python
page = (
    df
    .orderBy("id")
    .offset(1000)
    .limit(1000)
)
```

Page 3:

```python
page = (
    df
    .orderBy("id")
    .offset(2000)
    .limit(1000)
)
```

Spark 4.2.0 documents `DataFrame.offset(num)` as skipping the first `num` rows.

---

# 16. Always Order Before Pagination

Pagination should generally use:

```python
df.orderBy("id").offset(offset).limit(page_size)
```

rather than:

```python
df.offset(offset).limit(page_size)
```

Without a deterministic ordering, there is no reliable definition of which rows belong to a particular page.

For a unique key:

```python
df.orderBy("id")
```

For a composite ordering:

```python
df.orderBy("created_at", "id")
```

The second column can provide deterministic tie-breaking when `created_at` is not unique.

---

# 17. Jupyter Pagination

A convenient Jupyter helper is:

```python
def fetch_page(df, page_number, page_size=1000):
    offset = page_number * page_size

    return (
        df
        .orderBy("id")
        .offset(offset)
        .limit(page_size)
        .toPandas()
    )
```

Then:

```python
page0 = fetch_page(df, 0)
page1 = fetch_page(df, 1)
page2 = fetch_page(df, 2)
```

Each page is converted independently to Pandas.

This is significantly safer than:

```python
df.toPandas()
```

for a very large result.

---

# 18. Test Dataset

We created a 1-million-row test DataFrame using Spark's `range()`:

```python
from pyspark.sql import functions as F

df = (
    spark.range(1_000_000)
    .withColumn("value", F.rand(seed=42))
    .withColumn(
        "category",
        F.when(F.col("value") < 0.25, "A")
         .when(F.col("value") < 0.50, "B")
         .when(F.col("value") < 0.75, "C")
         .otherwise("D")
    )
)

df.createOrReplaceTempView("test_large_result")
```

The resulting temporary view contains:

```text
1,000,000 rows
```

We can query it using:

```python
df = spark.table("test_large_result")
```

and paginate it using:

```python
page = (
    df
    .orderBy("id")
    .offset(1000)
    .limit(1000)
)
```

---

# 19. OFFSET Pagination vs Keyset Pagination

OFFSET pagination is simple:

```text
ORDER BY id
OFFSET 1,000,000
LIMIT 1,000
```

However, for very deep pages, the engine may need to process a substantial amount of data before producing the requested page.

For large datasets, **keyset/seek pagination** can be preferable.

Example:

```python
page1 = (
    df
    .orderBy("id")
    .limit(1000)
)
```

Suppose the last row has:

```text
id = 999
```

Then fetch the next page:

```python
page2 = (
    df
    .filter(df.id > 999)
    .orderBy("id")
    .limit(1000)
)
```

Then:

```python
page3 = (
    df
    .filter(df.id > 1999)
    .orderBy("id")
    .limit(1000)
)
```

Conceptually:

```text
Page 1
id <= 999
       │
       ▼
Page 2
id > 999 AND id <= 1999
       │
       ▼
Page 3
id > 1999 AND id <= 2999
```

For interactive applications with very large datasets, keyset pagination is generally a better design than allowing arbitrary deep OFFSET values.

---

# 20. Result Streaming with `toLocalIterator()`

Pagination is not the only way to handle a large result.

PySpark also provides:

```python
df.toLocalIterator()
```

Example:

```python
for row in df.toLocalIterator():
    process(row)
```

This returns an iterator rather than materializing the complete result as a Python list.

The PySpark documentation states that `toLocalIterator()` consumes memory approximately corresponding to the largest partition. It supports Spark Connect starting with PySpark 3.4.0.

Therefore:

```python
df.collect()
```

and:

```python
df.toLocalIterator()
```

have very different memory characteristics.

### `collect()`

```text
Spark
  │
  └── ALL rows
        │
        ▼
     Python memory
```

### `toLocalIterator()`

```text
Spark
  │
  ├── partition/batch
  │       ▼
  │    Python
  │
  ├── next partition
  │       ▼
  │    Python
  │
  └── ...
```

For very large results that need to be processed sequentially, `toLocalIterator()` is therefore a useful alternative.

---

# 21. Pagination vs Streaming

The two approaches solve different problems.

| Requirement | Recommended approach |
|---|---|
| Display 100–5,000 rows in Jupyter | `limit()` |
| Interactive page 1/2/3 | `orderBy().offset().limit()` |
| Very deep pagination | Keyset pagination |
| Process entire large result | `toLocalIterator()` |
| Convert one page to Pandas | `limit().toPandas()` |
| Convert millions of rows to Pandas | Avoid |
| Export huge result | Write to Parquet/Delta/files |
| Build a web/Jupyter data browser | Page with bounded result sizes |

---

# 22. Recommended Architecture for Jupyter

For a Jupyter-based data platform, the recommended architecture is:

```text
                         Jupyter
                            │
                            │ gRPC
                            ▼
                  Spark Connect Server
                            │
                 ┌──────────┴──────────┐
                 │                     │
          Connect Session        Connect Session
                 │                     │
             user_id              user_id
             session_id           session_id
                 │                     │
                 └──────────┬──────────┘
                            │
                      Spark Application
                            │
                 ┌──────────┴──────────┐
                 │                     │
             Executors             Executors
```

For result retrieval:

```text
Jupyter
   │
   │ execute query
   ▼
Spark Connect
   │
   ├── Page 1 ──► Pandas
   ├── Page 2 ──► Pandas
   ├── Page 3 ──► Pandas
   │
   └── or
         │
         └── toLocalIterator()
                  │
                  ▼
             process rows
```

---

# 23. Recommended Session Metadata Convention

For a multi-user Jupyter platform, a useful convention is:

```text
user_id
    │
    └── Identity of the human/service user

session_id
    │
    └── Unique Spark Connect session

user_agent
    │
    └── Client/application, e.g. jupyter

app_name
    │
    └── Spark application name

operation tags
    │
    ├── project
    ├── notebook
    ├── request
    └── team
```

Example:

```python
import uuid

session_id = str(uuid.uuid4())

spark = (
    SparkSession.builder
    .remote(
        f"sc://localhost:15002/"
        f";user_id=ervijay"
        f";session_id={session_id}"
        f";user_agent=jupyter"
    )
    .appName("Customer Analytics")
    .getOrCreate()
)

spark.addTag("project:customer-analytics")
spark.addTag("notebook:customer-analysis")
spark.addTag("team:data-platform")
```

This gives a clean separation between:

```text
Identity
    user_id

Session
    session_id

Client
    user_agent

Application
    app_name

Operations
    tags
```

---

# 24. Key Findings

### Architecture

1. Spark Connect separates the Python/Jupyter client from the Spark driver.
2. Communication occurs through gRPC.
3. The Connect server can manage multiple client sessions.
4. Multiple Connect sessions can exist under one Spark application.

### Session management

1. `user_id` identifies the Connect user context.
2. `session_id` identifies the Connect session.
3. `session_id` defaults to a generated UUID.
4. `user_id` and `session_id` should be treated as Connect-session identity.
5. `user_agent` identifies the client/application.
6. `.appName()` sets Spark application metadata, not Connect session metadata.
7. `spark.stop()` releases the current remote Connect session; it does not shut down the shared remote Spark Connect server/application.
8. Operation tags provide useful execution-level metadata.
9. gRPC keepalive is connection-liveness functionality, not equivalent to a session idle timeout.

### Result retrieval

1. PySpark 4.2.0 supports `DataFrame.offset()`.
2. Pagination should normally use deterministic `orderBy()`.
3. `offset + limit` is convenient for interactive pagination.
4. Keyset pagination is preferable for very deep pagination over large datasets.
5. `toLocalIterator()` provides an incremental way to consume a large result.
6. `collect()` and unrestricted `toPandas()` should be avoided for very large result sets.

---

# 25. Overall Design

The resulting architecture can be summarized as:

```text
                         ┌───────────────────────┐
                         │       Jupyter         │
                         │                       │
                         │ user_id = ervijay     │
                         │ user_agent = jupyter  │
                         │ session_id = UUID     │
                         └───────────┬───────────┘
                                     │
                                  gRPC
                                     │
                                     ▼
                         ┌───────────────────────┐
                         │  Spark Connect Server │
                         │                       │
                         │  Session Management   │
                         └───────────┬───────────┘
                                     │
                                     ▼
                         ┌───────────────────────┐
                         │   Spark Application   │
                         │                       │
                         │ app_id                │
                         │ app_name              │
                         └───────────┬───────────┘
                                     │
                         ┌───────────┴───────────┐
                         │                       │
                         ▼                       ▼
                    Executors              Executors


Result retrieval:

                 Spark DataFrame
                       │
          ┌────────────┼─────────────┐
          │            │             │
          ▼            ▼             ▼
       Page 1       Page 2        Page N
     offset/limit  offset/limit  offset/limit
          │            │             │
          └────────────┼─────────────┘
                       ▼
                    Jupyter


For very large sequential results:

                 Spark DataFrame
                       │
                       ▼
               toLocalIterator()
                       │
                       ▼
              incremental processing
```

The central design principle is:

> **Treat Spark Connect session identity, Spark application identity, and result retrieval as three separate concerns.**

This makes the architecture much easier to reason about and scales better when Jupyter becomes a multi-user platform.