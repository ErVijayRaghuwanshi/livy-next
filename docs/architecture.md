# Architecture & Design Specifications

Welcome to the architectural specifications and designs for **`livy-next`**.

To make technical reviews and extensions simpler, the system documentation is divided into the following specialized design artifacts:

---

## Technical Specifications Index

### 🌐 [High-Level Design (HLD)](file:///Users/ervijay/Documents/Programs/Repo/livy-next/docs/hld.md)
*Focuses on system topologies, network boundaries, deployment architectures, and end-to-end request sequence diagrams.*
* **Contents**:
  * System Context & Network Topology.
  * Interactive Session Creation sequence flow.
  * Asynchronous SQL Statement submission and client polling sequences.

---

### ⚙️ [Low-Level Design (LLD)](file:///Users/ervijay/Documents/Programs/Repo/livy-next/docs/lld.md)
*Dives deep into package structures, internal code architectures, concurrency safety models, and data translations.*
* **Contents**:
  * Component Class Diagram (struct relationships).
  * State Machine transition logic for Session and Statement loops.
  * Mutex safety patterns and non-blocking background workers.
  * Spark-to-Livy Arrow type system mapping mappings.

---

### 🚀 [Project Roadmap](file:///Users/ervijay/Documents/Programs/Repo/livy-next/docs/roadmap.md)
*Tracks project milestones, past achievements, and future planning phases.*
* **Contents**:
  * Timeline Gantt Chart of development.
  * Phase 1 to Phase 4 detailed milestone lists (including local REPL process injection strategies).
