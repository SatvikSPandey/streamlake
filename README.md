# StreamLake — Real-Time Streaming Data Lakehouse

> Production-shape data platform that ingests simulated OTT user events through Kafka, processes them with Spark Structured Streaming into a Medallion-architecture Delta Lake on S3, loads the Gold layer into Snowflake, orchestrates the whole thing with Prefect, and exposes two complementary dashboards — Grafana for always-on BI and Streamlit for interactive analyst exploration.

**Author:** Satvik Pandey · **Portfolio project** · **Not affiliated with any company**

---

## 🔗 Live Demo

| Surface | URL | Purpose |
|---|---|---|
| 🎬 **Streamlit Dashboard** | **[streamlake-satvik.streamlit.app](https://streamlake-satvik.streamlit.app)** | Interactive analyst exploration with filters, drill-downs, CSV download |
| 📊 Grafana Dashboard | Local Docker (see [grafana/README.md](grafana/README.md)) | Always-on executive BI over Snowflake Gold |
| 💾 Source | [github.com/SatvikSPandey/streamlake](https://github.com/SatvikSPandey/streamlake) | This repository |

---

## Why this project exists

Modern streaming-media platforms (Netflix, JioStar, Hotstar, Disney+) generate billions of user interaction events per day — play, pause, skip, seek, watch-duration — and turn those events into real-time recommendations, content analytics, ad targeting, QoS monitoring, and business intelligence.

StreamLake is a working reference implementation of that architecture, built end-to-end at portfolio scale using the same tools a production OTT platform would actually deploy. Every piece runs on real infrastructure — Confluent Cloud Kafka, AWS S3, Snowflake warehouse, Streamlit Cloud — not mocked or faked.

---

## Architecture

```text
┌─────────────────────────┐
│  Event Producer         │  Python — synthetic OTT events
│  (Pydantic schema)      │  (play / pause / seek / complete / etc.)
└──────────┬──────────────┘
           │ JSON events
           ▼
┌─────────────────────────┐
│  Apache Kafka           │  Confluent Cloud (AWS ap-south-1 Mumbai)
│  Topic: user-events     │  6 partitions, Schema Registry
└──────────┬──────────────┘
           │ stream
           ▼
┌─────────────────────────┐
│  Spark Structured       │  Local PySpark 3.5 + delta-spark 3.2
│  Streaming              │  Kafka connector, Hadoop-AWS, Delta
└──────────┬──────────────┘
           │
           ▼
┌────────────────────────────────────────────────────────────────┐
│             DELTA LAKE on AWS S3 (ap-south-1 Mumbai)           │
│  ┌────────────┐      ┌────────────┐      ┌────────────┐        │
│  │  BRONZE    │  ──▶ │  SILVER    │  ──▶ │   GOLD     │        │
│  │ raw events │      │ deduped    │      │ 5 aggregate│        │
│  │ partitioned│      │ validated  │      │ tables     │        │
│  │ by user_id │      │ by date    │      │            │        │
│  └────────────┘      └────────────┘      └──────┬─────┘        │
└──────────────────────────────────────────────────┼─────────────┘
                                                   │
                                                   ▼
                                     ┌─────────────────────────┐
                                     │   Snowflake             │
                                     │   (ap-southeast-1 SG)   │
                                     │   STREAMLAKE_DB.GOLD    │
                                     └──────┬──────────────────┘
                                            │
                       ┌────────────────────┴────────────────────┐
                       ▼                                         ▼
            ┌─────────────────────┐                  ┌─────────────────────┐
            │  Grafana OSS        │                  │  Streamlit Cloud    │
            │  (Docker, local)    │                  │  (public URL)       │
            │  Executive BI       │                  │  Analyst exploration│
            └─────────────────────┘                  └─────────────────────┘

                     ┌─────────────────────────────────────────┐
                     │   Prefect 3.6 (orchestration)           │
                     │   silver → gold → snowflake flow        │
                     └─────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Rationale |
|---|---|---|
| Ingestion | Apache Kafka (Confluent Cloud) | Industry-standard event streaming, partitioned, replayable |
| Schema | Pydantic v2 | Type-safe event validation before Kafka publish |
| Processing | Spark Structured Streaming 3.5 | Exactly-once semantics, unified batch + stream API |
| Storage | Delta Lake 3.2 on AWS S3 | ACID transactions on data lake, schema evolution, time travel |
| Architecture | Medallion (Bronze / Silver / Gold) | Industry best-practice pattern for progressive refinement |
| Warehouse | Snowflake (Enterprise trial) | Sub-second BI queries on Gold aggregates |
| Orchestration | Prefect 3.6 | Python-native DAG orchestration with UI observability |
| BI Dashboard | Grafana OSS 11.4 (self-hosted Docker) | Always-on dashboards over Snowflake via Michelin community plugin |
| Analyst Dashboard | Streamlit 1.39 + Plotly | Interactive exploration with filters, drill-down, CSV download |
| Language | Python 3.11 | Pinned for PySpark 3.5 compatibility |
| JVM | Microsoft OpenJDK 17 LTS | Matches Databricks runtime, supported through 2029 |

---

## Project phases

Each phase is a distinct commit on `main` with a complete feature:

| # | Phase | What it delivers | Key files |
|---|---|---|---|
| 1 | **Setup** | Python 3.11 venv, JDK 17, Hadoop winutils, PySpark sanity check | `test_spark.py`, `requirements.txt` |
| 2 | **Kafka Producer** | Pydantic event schema, state-machine event generator, Streamlit control panel | `producer/`, `dashboard/producer_control.py` |
| 3 | **Bronze ingestion** | Spark reads Kafka → parses JSON → writes Bronze Delta partitioned by user_id | `spark/ingest_to_bronze.py` |
| 4 | **Silver + Gold** | Dedupe on event_id, validation, 5 Gold aggregate tables | `spark/transform_to_silver.py`, `spark/transform_to_gold.py` |
| 5 | **Snowflake** | DDL for Gold tables, loader via Rust deltalake + pandas write_pandas | `snowflake/setup_ddl.sql`, `snowflake/load_gold.py` |
| 6 | **Orchestration** | Prefect flow wraps Silver → Gold → Snowflake with retries, dependency graph, UI | `orchestration/flow.py` |
| 7 | **Grafana BI** | Self-hosted Docker Grafana with Michelin Snowflake plugin, 5-panel dashboard | `grafana/docker-compose.yml`, `grafana/dashboard.json` |
| 8 | **Streamlit analytics** | Interactive dashboard deployed to Streamlit Cloud, live public URL | `dashboard/analytics.py` |
| 9 | **Documentation** | This README, architecture diagrams, setup instructions, design notes | `README.md` |

---

## Dashboards — two tools, two audiences

StreamLake ships **two** visualization layers over the same Snowflake Gold backend. This is intentional, not redundant:

### Grafana → Executive BI (always-on)
- Runs locally via Docker, connects to Snowflake via community plugin
- 5 panels: Session Summary (stat cards), Device Mix (pie), Event Type Distribution (bar), Top Content (horizontal top-10), Watch Time by Country (bar)
- Audience: leadership checking "is the pipeline healthy and what are the numbers?"
- See [grafana/README.md](grafana/README.md) for setup

### Streamlit → Analyst exploration (interactive)
- Deployed publicly at [streamlake-satvik.streamlit.app](https://streamlake-satvik.streamlit.app)
- 5 sections: KPI overview, country-filtered watch time, sliderable content leaderboard, device mix + breakdown table, session explorer with search and CSV download
- Audience: analysts drilling into data, comparing segments, exporting for offline work

Tool choice rationale: Grafana is purpose-built for always-on BI with scheduled refresh; Streamlit is purpose-built for ad-hoc interactive exploration. Different UX philosophies, different strengths.

---

## Key Design Decisions

### Pivoted from Databricks Community Edition to local PySpark

Databricks Community Edition was replaced by **Databricks Free Edition** in early 2026 — Free Edition only offers SQL Warehouses + Serverless compute, no all-purpose clusters, and can't install custom JARs. The Kafka connector JAR we need for streaming ingestion isn't installable.

Chose local PySpark 3.5 with manual JAR management (`spark.jars.packages` config). More setup work, but gives us full Spark capability locally and the same behavior Databricks would show in a paid tier.

### Pivoted from Grafana Cloud to self-hosted Grafana via Docker

Grafana Cloud free tier has a documented UX bug ([grafana/grafana#73157](https://github.com/grafana/grafana/issues/73157)) where Enterprise plugins (including the official Snowflake plugin) can't be installed through any working UI path despite being advertised as free on Cloud. Self-hosted OSS Grafana + Michelin's Apache-licensed community plugin is the production-realistic workaround and is what most teams actually run for local dev.

### Subprocess isolation in Prefect tasks

Each Spark-driven task in the Prefect flow spawns a subprocess rather than importing scripts inline. Reasons: (1) Spark needs a fresh JVM per run — reusing SparkSession across different pipelines causes config conflicts; (2) error isolation — if Silver OOMs, Gold and Snowflake stages still get clean state; (3) matches production reality where each task is a separate container.

### `deltalake` Rust library for Snowflake loading, not PySpark

The Snowflake loader reads Gold Delta tables from S3 using the `deltalake` Python library (Rust-based), converts to pandas, then `write_pandas` to Snowflake. Avoids spinning up Spark JVM just to move 268 rows. Fast and dependency-light.

### Python 3.11 pinned runtime

PySpark 3.5 was compiled against Python 3.10-3.11. Python 3.12/3.13 exposes undocumented pyarrow/py4j issues on Windows. 3.11 supported through October 2027.

### NumPy pinned below 2.0

PySpark 3.5 was built against NumPy 1.x C API. NumPy 2.x breaks with `AttributeError: _ARRAY_API not found`. `numpy==1.26.4` is the safe pin.

### `PYSPARK_PYTHON = sys.executable`

Windows' App Execution Alias routes bare `python` to a Microsoft Store stub, causing Spark workers to fail silently. Forcing `PYSPARK_PYTHON = sys.executable` at every entry point ensures workers run in the same venv as the driver.

### Project path kept short, ASCII-only, outside OneDrive

Non-ASCII characters corrupt PySpark's Windows classpath. Spaces break Spark's batch script quoting. OneDrive's sync engine creates reparse points that conflict with Spark's continuous checkpoint writes. `C:\work\streamlake` sidesteps all three.

---

## Prerequisites

- **Windows 10/11** (tested with PowerShell; Linux/macOS should work with minor adjustments)
- **Python 3.11** (NOT 3.12/3.13)
- **Microsoft OpenJDK 17 LTS**
- **Docker Desktop** (for Grafana, Phase 7)
- **Git**
- ~2 GB free disk space

### Cloud accounts needed (all free tiers)

- **Confluent Cloud** — Kafka (Phase 2)
- **AWS** — S3 data lake storage (Phase 3)
- **Snowflake** — 30-day trial with $400 credit (Phase 5)
- **Streamlit Community Cloud** — dashboard deployment (Phase 8, GitHub SSO)

---

## Setup

> **Windows note:** Keep the project path short, ASCII-only, and outside OneDrive. `C:\work\streamlake` is ideal. See Design Decisions above for why.

### 1. Clone and set up Python

```powershell
git clone https://github.com/SatvikSPandey/streamlake.git C:\work\streamlake
cd C:\work\streamlake

py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Install Microsoft OpenJDK 17 LTS

Download from <https://learn.microsoft.com/en-us/java/openjdk/download>. During install, enable all four features: Add to PATH, Set JAVA_HOME, Associate .jar, JavaSoft registry keys.

Verify:

```powershell
java -version   # must show 17.0.x
$env:JAVA_HOME  # must show JDK 17 path
```

### 3. Install Hadoop winutils (Windows only)

```powershell
New-Item -ItemType Directory -Path "C:\hadoop\bin" -Force
Invoke-WebRequest "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe" -OutFile "C:\hadoop\bin\winutils.exe"
Invoke-WebRequest "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll" -OutFile "C:\hadoop\bin\hadoop.dll"
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
$userPath = [Environment]::GetEnvironmentVariable("PATH", "User")
[Environment]::SetEnvironmentVariable("PATH", "$userPath;C:\hadoop\bin", "User")
```

Restart PowerShell to pick up env vars.

### 4. Configure secrets

```powershell
Copy-Item imp\.env.example imp\.env
# Edit imp\.env with real credentials — NEVER commit it
```

### 5. Verify installation

```powershell
python test_spark.py
```

Expect a DataFrame and a per-user aggregation, ending with `PySpark environment is WORKING.`

---

## Running the pipeline

### Full data flow (one-time or on-demand)

```powershell
# 1. Start Kafka producer (new terminal)
streamlit run dashboard\producer_control.py
#   → open localhost:8501, click "Start streaming", produce 500+ events

# 2. Ingest Kafka → Bronze (new terminal)
python spark\ingest_to_bronze.py

# 3. Run batch refresh via Prefect (new terminal)
python orchestration\flow.py
#   → runs Silver → Gold → Snowflake load end-to-end with retries and logging
```

### Dashboards

```powershell
# Grafana (Docker)
cd grafana
docker compose up -d
# → open localhost:3000, admin/admin, import dashboard.json

# Streamlit (local)
streamlit run dashboard\analytics.py
# → open localhost:8501

# Streamlit (public)
# Already deployed at https://streamlake-satvik.streamlit.app
```

### Prefect UI (optional)

```powershell
prefect server start
# → open localhost:4200 to see flow runs, task timings, retry history
```

---

## Project Structure

```text
streamlake/
├── imp/                           # Secrets (gitignored except .env.example)
│   └── .env.example
├── .streamlit/                    # Streamlit config (secrets.toml gitignored)
├── producer/                      # Phase 2: Kafka event producer
│   ├── event_schema.py            # Pydantic v2 UserEvent
│   ├── event_generator.py         # State-machine event synthesis
│   ├── kafka_producer.py          # Confluent Cloud publisher
│   └── test_produce_one.py
├── spark/                         # Phases 3-4: Spark jobs
│   ├── ingest_to_bronze.py        # Kafka → Bronze Delta on S3
│   ├── transform_to_silver.py     # Bronze → Silver (dedupe + validate)
│   └── transform_to_gold.py       # Silver → 5 Gold aggregate tables
├── snowflake/                     # Phase 5: warehouse layer
│   ├── setup_ddl.sql              # Gold table DDL
│   ├── run_setup.py               # DDL runner
│   └── load_gold.py               # S3 Delta → Snowflake loader
├── orchestration/                 # Phase 6: Prefect flows
│   └── flow.py                    # streamlake_batch_refresh DAG
├── grafana/                       # Phase 7: self-hosted BI
│   ├── docker-compose.yml         # Grafana OSS + Snowflake plugin
│   ├── dashboard.json             # 5-panel OTT analytics dashboard
│   └── README.md                  # Phase 7 setup notes
├── dashboard/                     # Phases 2 & 8: Streamlit apps
│   ├── producer_control.py        # Phase 2 control panel
│   └── analytics.py               # Phase 8 public analytics dashboard
├── requirements.txt               # Pinned deps
├── test_spark.py                  # Phase 1 sanity check
├── .gitignore
└── README.md
```

---

## What the data looks like

Synthetic OTT events with realistic distributions:

- **12 countries** with India dominant (matches JioStar market focus)
- **5 device types**: mobile_android (46%), smart_tv (20%), mobile_ios (18%), web (12%), tablet (5%)
- **7 event types**: play, pause, seek, resume, skip, error, complete
- **~850 events** produced across 128 unique users into 142 sessions in the reference dataset

The Gold tables aggregate these into 5 business-ready views:

1. `DAILY_WATCH_TIME_BY_COUNTRY` — 12 rows, watch time per country per day
2. `TOP_CONTENT_BY_WATCH_TIME` — 102 rows, content leaderboard
3. `HOURLY_EVENT_DISTRIBUTION` — 7 rows, event type counts per hour
4. `DAILY_DEVICE_MIX` — 5 rows, device breakdown per day
5. `USER_SESSION_SUMMARY` — 142 rows, one row per session with full drill-down columns

---

## Lessons learned

### "Made it work locally" is not the same as "ready to deploy"

Streamlit Cloud does a fresh pip install on every deploy with latest-compatible versions. Your local venv was resolved months ago with older compatible versions frozen in. The cryptography 46 / pyopenssl 22 clash only surfaced on deploy. Pinning direct deps isn't always enough — sometimes you pin a transitive one too.

### Grafana Cloud's "free forever Enterprise plugins" has documented bugs

Advertised: "all Enterprise plugins free on Cloud." Reality: Snowflake plugin install buttons don't surface on free tier workspaces. GitHub issue [#73157](https://github.com/grafana/grafana/issues/73157) open since 2023. Self-hosted + community plugin is the real-world workaround.

### Snowflake column names are always UPPERCASE

Unquoted SQL identifiers get uppercased at parse time. `SELECT country FROM ...` looks for a column literally named `COUNTRY`, not whatever you wrote. Always uppercase in SQL unless you explicitly created with double quotes.

### `INFORMATION_SCHEMA.COLUMNS` is ground truth

When a query returns "No data" with no error, trust `INFORMATION_SCHEMA.COLUMNS` over memory, over truncated UI screenshots, over the code that wrote the table. Saves 30 min of guessing per mystery.

### Subprocess > import for Spark orchestration

Don't import Spark-running scripts into a Prefect flow. Subprocess them. Fresh JVM per task, clean state, matches production containerization, easier to debug.

---

---

## Production Deployment Plan

> This section documents how StreamLake's local pipeline would deploy to AWS EMR for production-scale workloads. The architecture is designed for this transition from day one — same Spark configs, same Delta Lake versions, same S3 bucket. The `emr/` directory contains the deployment topology as executable infrastructure documentation.
>
> **Status:** Local pipeline fully operational. EMR configs documented in `emr/` — not yet executed on a live cluster.

### Why the local pipeline scales to EMR without redesign

Every architectural decision in StreamLake was made with production scale in mind:

- **Partitioning by `user_id` and `event_date`** means Spark distributes work evenly across executors. At 10M users, each partition handles a predictable slice — no hot partitions, no stragglers. Adding EMR core nodes increases parallelism linearly.
- **Delta Lake ACID transactions** mean SPOT instance interruptions don't corrupt data. EMR can safely use SPOT nodes (70% cheaper) because Delta's write-ahead log guarantees atomicity — a failed write leaves the table in the last committed state, not a half-written mess.
- **Kafka's 6-partition `user-events` topic** maps to 6 Spark tasks per micro-batch today. Scaling to 600 partitions (matching 600 Confluent Cloud partitions) would give 600 parallel Spark tasks — linear throughput increase with zero code changes. Kafka partition count is the single dial that controls ingestion parallelism.
- **`spark.sql.shuffle.partitions=200`** in `emr/cluster_config.json` matches EMR core node count × executor cores. At 10 nodes × 8 vCPU × 2 executors = 160 executors — 200 shuffle partitions keeps all executors busy with minimal idle time.

### EMR deployment files

| File | Purpose |
|---|---|
| `emr/cluster_config.json` | EMR cluster definition: m5.xlarge master, m5.2xlarge SPOT core nodes, auto-scaling 2→10 nodes, Spark config matching local settings exactly |
| `emr/bootstrap.sh` | Node bootstrap: installs Python 3.11, all pinned dependencies from requirements.txt, sets PYSPARK_PYTHON on every node before Spark launches |
| `emr/submit_job.sh` | spark-submit commands for Bronze ingestion, Silver transform, Gold transform — uploads scripts to S3, submits as EMR steps via AWS CLI |

### Auto-scaling policy

The cluster scales out when YARN memory utilisation exceeds 75% for 5 minutes (adds 2 nodes) and scales in when utilisation drops below 40% for 15 minutes (removes 1 node). This handles the OTT traffic pattern — evening primetime spikes followed by overnight quiet periods — without paying for idle capacity overnight.

| Scenario | Nodes | vCPU | RAM | Events/day estimate |
|---|---|---|---|---|
| Development (local) | 1 (simulated) | 8 | 16GB | ~10K |
| EMR minimum (2 core) | 3 total | 20 | 80GB | ~5M |
| EMR auto-scaled (10 core) | 11 total | 84 | 336GB | ~500M |
| EMR max with partition scaling | 11 + 600 Kafka partitions | 84 | 336GB | ~2B+ |

### Delta Lake at scale — OPTIMIZE and ZORDER

At production volumes, Delta Lake tables accumulate thousands of small files per partition (the "small file problem"). Two Delta maintenance operations keep query performance fast:

```sql
-- Compact small files in Silver table into larger, query-efficient files
OPTIMIZE delta.`s3://streamlake-data-lake/silver/` ZORDER BY (user_id, event_date);

-- Compact Gold aggregates — run after each batch refresh
OPTIMIZE delta.`s3://streamlake-data-lake/gold/user_session_summary/` ZORDER BY (country_code, device_type);
```

`ZORDER BY (user_id, event_date)` co-locates data for the most common query patterns (per-user drill-down, per-day aggregation) in the same files — Spark skips entire files that don't match a filter, reducing S3 read volume by 60-80% at scale.

### Prefect in production

The local `orchestration/flow.py` already runs Silver → Gold → Snowflake as a dependency graph with retries. On EMR, the same Prefect flow would replace the `subprocess` calls with `aws emr add-steps` calls from `emr/submit_job.sh` — each EMR step becomes a Prefect task. Prefect's retry logic handles SPOT interruptions; Delta Lake's ACID guarantees handle partial writes.

## Author

**Satvik Pandey** — AI / Python Engineer with 4+ years of experience building LLM systems, data pipelines, and production backends.

- GitHub: [@SatvikSPandey](https://github.com/SatvikSPandey)
- LinkedIn: [satvikpandey-433555365](https://www.linkedin.com/in/satvikpandey-433555365)
- Portfolio: [satvikspandey.netlify.app](https://satvikspandey.netlify.app)
- Email: satvikpan@gmail.com

---

## License

MIT
