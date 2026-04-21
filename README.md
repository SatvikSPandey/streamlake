# StreamLake — Real-Time Streaming Data Lakehouse

> A production-shape data platform that ingests high-volume user interaction
> events through Kafka, processes them with Spark Structured Streaming, and
> persists them into a Medallion-architecture Delta Lake on S3 — with a
> Snowflake warehouse layer and live dashboards on top.
>
> Built as a self-directed freelance portfolio project by Satvik Pandey.

**Status:** 🚧 Phase 1 complete (local environment verified). Phases 2–9 in progress.

---

## About This Project

StreamLake is a working reference implementation of the data pipeline
architecture used by modern streaming-media and OTT platforms. These platforms
generate billions of user interaction events per day — play, pause, skip,
seek, watch-duration — and turn those events into real-time recommendations,
content analytics, ad targeting, QoS monitoring, and business intelligence.

This project implements that pipeline end-to-end at a learnable scale, using
the same tooling and architectural patterns that a production streaming
platform would deploy. It is a personal build — not affiliated with or
commissioned by any company.

---

## Architecture

┌──────────────────────┐
│  Event Producer      │  Python — simulated viewer events
│  (Kafka Producer)    │  (play / pause / skip / watch_duration)
└──────────┬───────────┘
│ JSON events
▼
┌──────────────────────┐
│  Apache Kafka        │  Confluent Cloud (managed Kafka)
│                      │  Topic: user-events
└──────────┬───────────┘
│ stream
▼
┌──────────────────────┐
│  Spark Structured    │  Micro-batch streaming
│  Streaming           │  (Databricks runtime)
└──────────┬───────────┘
│
▼
┌──────────────────────────────────────────────────────┐
│             DELTA LAKE on AWS S3                     │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐      │
│  │  BRONZE    │→ │  SILVER    │→ │   GOLD     │      │
│  │ raw events │  │ cleaned &  │  │ aggregated │      │
│  │            │  │ deduped    │  │ metrics    │      │
│  └────────────┘  └────────────┘  └────────────┘      │
└───────────────────────────┬──────────────────────────┘
│
▼
┌──────────────────┐
│    Snowflake     │  Warehouse for BI / analytics
│  (Gold queries)  │
└────────┬─────────┘
│
┌────────────┼─────────────┐
▼                          ▼
┌──────────────┐          ┌──────────────┐
│  Streamlit   │          │   Grafana    │
│  Dashboard   │          │  Pipeline    │
│  (live demo) │          │  Health      │
└──────────────┘          └──────────────┘

---

## Tech Stack

| Layer | Tool | Rationale |
|---|---|---|
| Ingestion | Apache Kafka (Confluent Cloud) | Industry-standard event streaming. High throughput, replayable, partitioned. |
| Processing | Spark Structured Streaming | Exactly-once semantics. Unified batch + stream API. |
| Storage | Delta Lake on AWS S3 | ACID transactions on a data lake. Schema evolution. Time travel. |
| Architecture | Medallion (Bronze / Silver / Gold) | Industry best-practice pattern for progressively refining data quality. |
| Warehouse | Snowflake | Sub-second BI queries on Gold-layer aggregates. |
| Orchestration | Prefect 3 | Modern Python-native workflow orchestration. |
| Monitoring | Grafana | Production-grade pipeline observability. |
| Demo UI | Streamlit | Fast live-demo surface for visualizing the pipeline. |
| Language | Python 3.11 | Pinned for PySpark / Delta Lake compatibility. |
| JVM | Microsoft OpenJDK 17 LTS | Current-generation LTS. Matches Databricks runtime. |

---

## Prerequisites

### Required on your machine

- **Windows 10/11** (tested on Windows with PowerShell; Linux/Mac should work with minor adjustments)
- **Python 3.11** (NOT 3.12 or 3.13 — PySpark 3.5.x compatibility)
- **Microsoft OpenJDK 17 LTS** (Java 8 works but is deprecated; Java 21+ breaks Spark 3.5)
- **Git**
- **~2 GB free disk space**

### Cloud accounts (free tiers used)

- **Confluent Cloud** — Kafka (Phase 2)
- **AWS** — S3 data lake storage (Phase 3)
- **Databricks Community Edition** — Spark runtime (Phase 3)
- **Snowflake** — 30-day free trial with $400 credit (Phase 5)

---

## Setup

> **Important for Windows users:** this project intentionally lives at a
> short, ASCII-only path outside of OneDrive. Spaces, em dashes (`—`), and
> cloud-sync folders are known to break PySpark on Windows. A path like
> `C:\work\streamlake` is ideal.

### 1. Clone the repository

```powershell
git clone https://github.com/SatvikSPandey/streamlake.git C:\work\streamlake
cd C:\work\streamlake
```

### 2. Install Microsoft OpenJDK 17 LTS

Download and install from: <https://learn.microsoft.com/en-us/java/openjdk/download>

During installation, enable all four features:
- Add to PATH
- Set JAVA_HOME
- Associate .jar
- JavaSoft (Oracle) registry keys

Verify:

```powershell
java -version   # must show 17.0.x
$env:JAVA_HOME  # must show the JDK 17 install path
```

### 3. Install Hadoop winutils (Windows-specific)

```powershell
New-Item -ItemType Directory -Path "C:\hadoop\bin" -Force
Invoke-WebRequest "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe" -OutFile "C:\hadoop\bin\winutils.exe"
Invoke-WebRequest "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll" -OutFile "C:\hadoop\bin\hadoop.dll"
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
$userPath = [Environment]::GetEnvironmentVariable("PATH", "User")
[Environment]::SetEnvironmentVariable("PATH", "$userPath;C:\hadoop\bin", "User")
```

Close and reopen PowerShell to pick up the new environment variables.

### 4. Create Python 3.11 virtual environment

```powershell
py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 5. Verify the installation

```powershell
python test_spark.py
```

You should see a small DataFrame of simulated viewer events and a per-user
aggregation, ending with:

PySpark environment is WORKING.

### 6. Set up secrets

Copy the template and fill in real credentials as you complete each phase:

```powershell
Copy-Item imp\.env.example imp\.env
# Edit imp\.env with real values — NEVER commit it
```

---

## Project Structure

streamlake/
├── imp/                    # Secrets (gitignored, except .env.example)
│   └── .env.example        # Template — safe to commit
├── producer/               # Kafka producer — simulated viewer events (Phase 2)
├── streaming/              # Spark Structured Streaming jobs
│   ├── bronze/             # Raw ingestion from Kafka → Delta (Phase 3)
│   ├── silver/             # Cleaning + deduplication (Phase 4)
│   └── gold/               # Business aggregations (Phase 4)
├── warehouse/              # Snowflake integration (Phase 5)
├── orchestration/          # Prefect flows (Phase 6)
├── monitoring/             # Grafana dashboard configs (Phase 7)
├── dashboard/              # Streamlit live demo (Phase 8)
├── notebooks/              # Databricks notebooks (exported .py)
├── config/                 # Non-secret configuration
├── docs/                   # Architecture diagrams and design notes
├── tests/                  # pytest suite
├── venv/                   # Python virtual environment (gitignored)
├── requirements.txt        # Pinned dependencies
├── test_spark.py           # Environment sanity check
├── .gitignore
└── README.md

---

## Roadmap

- [x] **Phase 1** — Project setup, Python 3.11 venv, Java 17, winutils, PySpark sanity test
- [ ] **Phase 2** — Confluent Cloud setup + Kafka event producer
- [ ] **Phase 3** — Databricks + Spark Structured Streaming → Bronze (Delta on S3)
- [ ] **Phase 4** — Silver + Gold Medallion transformations
- [ ] **Phase 5** — Snowflake warehouse integration
- [ ] **Phase 6** — Prefect orchestration
- [ ] **Phase 7** — Grafana monitoring
- [ ] **Phase 8** — Streamlit live dashboard
- [ ] **Phase 9** — Documentation, architecture diagrams

---

## Key Design Decisions

### Python 3.11 as the pinned runtime

PySpark 3.5.x was compiled against Python 3.10 and 3.11. Running on Python 3.12
or 3.13 exposes undocumented issues in `pyarrow` and `py4j` on Windows.
Pinning to Python 3.11.9 eliminates an entire class of compatibility friction
without sacrificing anything meaningful — 3.11 is supported until October 2027.

### NumPy pinned below 2.0

PySpark 3.5 was built against NumPy 1.x's C API. NumPy 2.x reorganized internal
symbols, which surfaces as `AttributeError: _ARRAY_API not found` at runtime.
`numpy==1.26.4` — the last 1.x release — is the safe pin until PySpark ships
NumPy 2.x support.

### `PYSPARK_PYTHON` explicitly set to `sys.executable`

Spark launches separate Python worker processes for distributed execution.
When those workers call `python` on PATH, Windows often routes the call to
the Microsoft Store App Execution Alias stub rather than the active venv
interpreter, causing workers to exit immediately with a misleading
"Python worker failed to connect back" socket timeout.

Setting `PYSPARK_PYTHON` (and `PYSPARK_DRIVER_PYTHON`) to `sys.executable`
at the top of every entry point forces workers to run in the same interpreter
as the driver — the venv Python. This is a portable, explicit solution that
avoids depending on system-wide settings.

### Project path kept short, ASCII-only, outside OneDrive

Three real issues drove this choice:
1. Non-ASCII characters (em dashes, accented letters) corrupt the classpath
   that PySpark constructs in Windows `cmd.exe`, causing Spark to fail with
   `ClassNotFoundException: org.apache.spark.deploy.SparkSubmit`.
2. Spaces in paths cause quoting issues in Spark's batch scripts.
3. OneDrive's sync engine creates reparse points and holds file locks that
   conflict with Spark's continuous checkpoint writes.

`C:\work\streamlake` sidesteps all three.

### Microsoft OpenJDK 17 over Oracle JDK or Java 8

- **Java 8** works with PySpark 3.5 but is end-of-life and doesn't match
  the production JVM used by Databricks or the Snowflake Spark connector.
- **Java 21+** is not supported by Spark 3.5.x.
- **Java 17 LTS** is the sweet spot — matches Databricks runtime, supported
  by Delta Lake 3.2.x and the Snowflake Spark connector, security-supported
  through 2029.

The **Microsoft Build of OpenJDK** specifically was chosen for its TCK
certification (behaviorally identical to Oracle JDK), Apache 2.0 licensing
(no commercial-use restrictions), and clean Windows MSI installer that
handles PATH and `JAVA_HOME` automatically.

### Dependencies declared up front, not captured post-hoc

`requirements.txt` was written before any `pip install`, with exact version
pins for every direct dependency. This makes the environment fully
reproducible from a fresh clone — the same approach any CI/CD pipeline takes.

### Medallion architecture (Bronze / Silver / Gold)

Bronze stores raw events untouched — the immutable source of truth that
allows downstream re-processing without re-ingesting from Kafka. Silver
applies cleaning, validation, and deduplication with schema enforcement.
Gold holds pre-aggregated business metrics ready for query. Separating the
three layers isolates concerns: a bug in Silver never corrupts Bronze, and
Gold schemas can evolve without re-processing raw data.

---

## Author

**Satvik Pandey** — AI / Python Engineer
- GitHub: [@SatvikSPandey](https://github.com/SatvikSPandey)
- LinkedIn: [satvikpandey-433555365](https://www.linkedin.com/in/satvikpandey-433555365)
- Portfolio: [satvikspandey.netlify.app](https://satvikspandey.netlify.app)
- Email: satvikpan@gmail.com

---

## License

MIT