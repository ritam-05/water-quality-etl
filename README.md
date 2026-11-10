# End-to-End Water Quality ETL Pipeline and BI

![Water quality monitoring buoy](docs/images/buoy.png)

An event-driven, serverless data pipeline for simulated water-quality telemetry.
The project generates sensor readings, reliably ingests them through Amazon SQS,
stores curated data in Supabase PostgreSQL, preserves compressed raw payloads in
Supabase Storage, and exposes operational and environmental insights through
Grafana.

## Contents

- [Project overview](#project-overview)
- [Architecture](#architecture)
- [End-to-end data flow](#end-to-end-data-flow)
- [Pipeline components](#pipeline-components)
- [Technologies used, in plain language](#technologies-used-in-plain-language)
- [Data model](#data-model)
- [Reliability and retention](#reliability-and-retention)
- [Data Visualization](#data-visualization---bi-layer)
- [Local development and deployment](#local-development-and-deployment)
- [Operations and security](#operations-and-security)
- [Scalability and scenarios](#scalability-and-scenarios)

## Project overview

### Goals

- Simulate time-series telemetry from distributed water-quality stations.
- Decouple data generation from processing with a durable queue.
- Store raw and curated data separately for traceability and analytics.
- Make message redelivery safe through idempotent writes.
- Retain only the most recent five days of raw and relational data.
- Visualize environmental readings and ingestion health in Grafana.

### What the pipeline produces

Each generated record includes a station identifier, region, event timestamp,
status, and measurements for pH, dissolved oxygen, turbidity, temperature,
specific conductance, and streamflow. A normal generator run produces 25
stations across five collection minutes, resulting in 125 readings in one SQS
message.

```mermaid
mindmap
  root((Water Quality ETL))
    Telemetry
      pH
      Dissolved oxygen
      Turbidity
      Temperature
      Conductance
      Streamflow
    Reliability
      Amazon SQS
      Idempotent inserts
      Dead-letter queue
    Storage
      Supabase PostgreSQL
      Supabase Storage
      Five-day retention
    Analytics
      Grafana dashboard
      Time-series panels
      Ingestion monitoring
```

## Architecture

![Water Quality ETL architecture](docs/images/arch-diagram.png)

```mermaid
flowchart LR
    subgraph AWS[AWS]
        GEN[Generator Lambda]
        QUEUE[water-quality-queue]
        PROC[Processor Lambda]
        DLQ[water-quality-dlq]
        CLEAN[Storage Cleanup Lambda]
        SCHED[EventBridge Scheduler]
    end

    subgraph Supabase[Supabase]
        PG[(PostgreSQL)]
        STORE[(Storage: raw-telemetry)]
        CRON[pg_cron]
    end

    GEN -->|JSON batch| QUEUE
    QUEUE -->|One message per invocation| PROC
    QUEUE -. failed after retries .-> DLQ
    PROC -->|Curated readings and audit rows| PG
    PROC -->|Compressed raw backup| STORE
    SCHED -->|Daily trigger| CLEAN
    CLEAN -->|Delete raw objects older than 5 days| STORE
    CRON -->|Delete relational rows older than 5 days| PG
```

## End-to-end data flow

### 1. Generate telemetry

`src/generator.py` simulates realistic water-quality values and intentionally
introduces anomalies and missing measurements for downstream testing. It sends
the JSON array to Amazon SQS rather than invoking the processor directly.

![Generator Lambda](docs/images/water-quality-generator-lambda.png)

### 2. Buffer and decouple with SQS

Amazon SQS absorbs bursts, separates producers from consumers, and retries
failed processing. The processor event-source mapping is configured with a
batch size of one so each invocation handles one 125-reading payload.

![SQS queue](docs/images/water-quality-sqs-queue.png)

### 3. Process and persist

`src/processor.py` connects to the Supabase transaction pooler, checks the
batch audit table, uploads a gzip raw-data backup, upserts station metadata,
inserts readings, and records the completed batch. The function uses a 120
second timeout and 256 MB memory allocation.

```mermaid
sequenceDiagram
    participant G as Generator Lambda
    participant Q as Amazon SQS
    participant P as Processor Lambda
    participant S as Supabase Storage
    participant D as Supabase PostgreSQL

    G->>Q: Send one JSON telemetry batch
    Q->>P: Deliver one SQS message
    P->>D: Look up message ID in pipeline_audit
    alt Audit row exists
        P-->>Q: Complete without duplicate work
    else New message
        P->>S: Upload compressed raw JSON backup
        loop Each telemetry reading
            P->>D: Upsert station metadata
            P->>D: Insert reading if event ID is new
        end
        P->>D: Insert audit row if batch ID is new
        P-->>Q: Complete successfully
    end
```

### 4. Serve analytics

Grafana connects through the Supabase **session pooler** on port `5432`. The
processor uses port `6543` because it is a short-lived serverless client;
Grafana uses the session pooler because it maintains database connections for
interactive dashboards.

```mermaid
flowchart LR
    DB[(Supabase PostgreSQL)] -->|Session pooler :5432| GRAF[Grafana PostgreSQL data source]
    GRAF --> DASH[Water Quality Dashboard]
    DASH --> ENV[Environmental metrics]
    DASH --> OPS[Ingestion and pipeline health]
```

### 5. Repository structure

The project keeps runtime code, database scripts, deployment configuration, and
documentation separate so the pipeline is easier to operate and extend.

| File or directory | Use |
|---|---|
| 📄 `README.md` | Project guide: architecture, setup, operations, dashboard queries, and scaling notes. |
| 📄 `requirements.txt` | Pins the Python dependencies needed for local work and Lambda packaging. |
| 📄 `.gitignore` | Prevents virtual environments, secrets, deployment bundles, and generated outputs from being committed. |
| 🚀 `deploy_processor.bat` | Packages `processor.py` with its dependencies and updates the processor Lambda. |
| ⚙️ `update_env.json` | Local AWS CLI input for processor environment variables. It is ignored because it contains secrets. |
| ⚙️ `cleanup_env.example.json` | Safe template showing the environment-variable names required by the cleanup Lambda. |
| ⚙️ `attributes.json` | AWS SQS configuration input, including the dead-letter queue redrive policy. |
| ⚙️ `targets.json` | AWS CLI target definition used when connecting a scheduled trigger to a Lambda. |
| 🔐 `trust-policy.json` | IAM trust policy that allows the configured AWS service to assume its execution role. |
| 🧪 `response.json` | Sample Lambda invocation response used to verify message generation. |
| 📂 `src/` | Python source code for the three Lambda responsibilities. |
| 🐍 `src/generator.py` | Simulates 125 water-quality readings per run and sends the batch to Amazon SQS. |
| 🐍 `src/processor.py` | Main ETL consumer: backs up raw JSON, upserts stations, writes readings, and records processing audit data. |
| 🐍 `src/storage_cleanup.py` | Scheduled retention worker that deletes raw Supabase Storage objects older than five days. |
| 📂 `sql/` | SQL scripts that create and maintain the Supabase PostgreSQL data layer. |
| 🗃️ `sql/schema.sql` | Creates the station, telemetry, alert, and audit tables, along with their indexes and constraints. |
| 🧹 `sql/rolling-cron-job.sql` | Creates the Supabase `pg_cron` task that removes expired relational data each day. |
| 📂 `docs/images/` | Visual assets embedded in this README, including AWS, Supabase, and Grafana evidence screenshots. |
## Pipeline components

| Component | Responsibility | Key behavior |
|---|---|---|
| Generator Lambda | Produces simulated sensor batches | Generates 125 readings per run and sends one message to SQS. |
| SQS queue | Buffers telemetry and retries failures | Batch size 1, visibility timeout 720 seconds, DLQ after configured retries. |
| Processor Lambda | Performs the ETL workflow | Uploads raw backup, writes curated rows, and records audit status. |
| Supabase PostgreSQL | Stores query-ready telemetry | Normalized schema with time-series indexes. |
| Supabase Storage | Stores immutable raw backups | Each SQS body is gzip-compressed and stored in `raw-telemetry`. |
| Cleanup Lambda | Enforces raw-object retention | Daily scheduled deletion of objects older than five days. |
| Supabase `pg_cron` | Enforces relational-data retention | Daily deletion of expired readings and audit rows. |
| Grafana | Visualizes data and operational behavior | PostgreSQL data source and public dashboard. |

## Technologies used, in plain language

This section explains the platform without assuming data-engineering experience.

| Technology | What it is | Why this project uses it |
|---|---|---|
| **Python** | A general-purpose programming language that is popular for automation and data work. | It powers the generator, processor, and cleanup functions. |
| **AWS Lambda** | Serverless compute: AWS runs small pieces of code only when they are needed. | The project does not need to run or maintain application servers continuously. |
| **Amazon SQS** | A managed message queue. Think of it as a durable waiting line between two systems. | It lets telemetry generation continue even if processing is temporarily slow or unavailable. |
| **Dead-letter queue (DLQ)** | A separate queue for messages that repeatedly fail processing. | It prevents one bad message from blocking the normal queue and preserves it for investigation. |
| **Supabase PostgreSQL** | A managed relational database based on PostgreSQL. Relational databases store structured records in linked tables. | It holds query-ready station, reading, and audit data for analytics and dashboards. |
| **Supabase Storage** | Managed object storage for files rather than database rows. | It preserves the original telemetry payload as a compressed backup before transformation. |
| **EventBridge Scheduler** | AWS's managed clock for running a task on a timetable. | It invokes the Storage cleanup Lambda once each day. |
| **Supabase `pg_cron`** | A PostgreSQL extension that schedules SQL commands inside the database. | It removes expired readings and audit records without requiring another application server. |
| **Grafana** | A visualization and dashboard platform for time-series and operational data. | It turns stored measurements into charts, tables, and ingestion-health panels. |
| **Gzip** | A file-compression format that reduces storage size. | Raw JSON messages are compressed before being saved to Supabase Storage. |
| **pg8000** | A pure-Python PostgreSQL database driver. A driver is the library an application uses to speak to a database. | The processor uses it to write telemetry to Supabase PostgreSQL from Lambda. |

> **About Parquet:** Parquet is a column-oriented file format commonly used for
> large-scale analytics because it is efficient for reading selected columns.
> This project does **not** currently use Parquet; raw payloads are stored as
> gzip-compressed JSON. Parquet would be a sensible future option if long-term
> historical data were exported to a data lake for large analytical workloads.

## Data model

```mermaid
erDiagram
    DIM_STATION ||--o{ WATER_QUALITY_READINGS : records
    DIM_STATION ||--o{ FACT_ALERTS : has

    DIM_STATION {
        varchar station_id PK
        varchar region
        timestamptz created_at
    }
    WATER_QUALITY_READINGS {
        uuid event_id PK
        varchar station_id FK
        timestamptz event_timestamp
        numeric ph
        numeric dissolved_oxygen
        numeric turbidity
        numeric temperature
        numeric specific_conductance
        numeric streamflow
        varchar status
        timestamptz ingested_at
    }
    PIPELINE_AUDIT {
        uuid batch_id PK
        timestamptz processed_at
        int records_processed
        varchar status
        text error_message
    }
    FACT_ALERTS {
        uuid alert_id PK
        varchar station_id FK
        timestamptz event_timestamp
        varchar parameter_name
        varchar alert_severity
        numeric reading_value
    }
```

### Supabase tables

#### `dim_station`

Station dimension data is inserted with `ON CONFLICT DO NOTHING`, allowing
many readings from the same station without duplicate metadata rows.

![Station dimension](docs/images/dim-station.png)

#### `water_quality_readings`

The fact table contains one row per telemetry event. `event_id` is the primary
key and makes repeated SQS deliveries harmless.

![Water quality readings](docs/images/water-quality-readings.png)

#### `pipeline_audit`

The audit table records completed SQS batches using the SQS message ID as the
primary key. It provides observability and prevents already-completed batches
from being processed again.

![Pipeline audit](docs/images/pipeline-audit.png)

## Reliability and retention

### Idempotency and retry behavior

SQS uses at-least-once delivery. A message can be delivered more than once,
particularly after a timeout or transient downstream failure. The processor
handles this safely in three layers:

1. It checks `pipeline_audit` before beginning work.
2. It ignores duplicate `event_id` values when inserting readings.
3. It ignores duplicate `batch_id` values when recording audit completion.

```mermaid
flowchart TD
    RECEIVE[Receive SQS message] --> CHECK{Batch ID already audited?}
    CHECK -->|Yes| ACK[Return success]
    CHECK -->|No| BACKUP[Upload raw backup]
    BACKUP --> WRITE[Write station and reading records]
    WRITE --> AUDIT[Write audit record]
    AUDIT --> ACK
    WRITE -. unrecoverable failure .-> RETRY[SQS retry]
    RETRY -. max receives exceeded .-> DLQ[Dead-letter queue]
```

### Five-day retention policy

Raw and relational data are retained independently so each storage layer can
be managed according to its purpose.

```mermaid
flowchart TB
    RAW[Supabase Storage raw objects] -->|created_at older than 5 days| CLEAN[Cleanup Lambda]
    CLEAN --> DELETE_RAW[Delete object paths]
    REL[PostgreSQL readings and audit rows] -->|older than 5 days| CRON[Supabase pg_cron]
    CRON --> DELETE_REL[Delete relational rows]
```

#### Raw Storage retention

`src/storage_cleanup.py` is deployed as
`water-quality-storage-cleanup`. EventBridge Scheduler invokes it every day at
03:00 in `Asia/Kolkata`. The handler paginates through the `raw-telemetry`
bucket, filters by `created_at`, and deletes eligible paths in batches of up to
1,000.

![Storage cleanup Lambda](docs/images/lambda-storage-cleanup.png)

#### Relational-data retention

Run [`sql/rolling-cron-job.sql`](sql/rolling-cron-job.sql) once in the Supabase
SQL Editor. It enables `pg_cron` and schedules daily cleanup at midnight UTC:

- `water_quality_readings` are removed when `event_timestamp` is older than
  five days.
- `pipeline_audit` records are removed when `processed_at` is older than five
  days.

> The supplied SQL intentionally retains `dim_station` and `fact_alerts`.
> Adjust the SQL if those tables require the same retention policy.

## Grafana dashboards

Grafana turns the curated PostgreSQL table into two complementary views: an
operational control centre for the latest pipeline data and an analytical
workspace for environmental diagnostics. All panel SQL is versioned in
[`sql/dashboard-queries.sql`](sql/dashboard-queries.sql).

### Data-source configuration

Use the Supabase **shared session pooler** in Grafana:

| Field | Value |
|---|---|
| Host URL | `aws-0-ap-southeast-1.pooler.supabase.com:5432` |
| Database | `postgres` |
| Username | `postgres.<project-ref>` |
| Password | Current Supabase database password in plain text, not a URI. |
| TLS/SSL mode | `require` |

Use the exact Session pooler host and username copied from the Supabase Connect
dialog for the project. Do not use the processor Lambda transaction-pooler
configuration (`:6543`) in Grafana.

### Dashboard 1: Operational Monitoring & Alerts

![Operational Monitoring & Alerts dashboard](docs/images/dashboard.png)

This dashboard focuses on real-time system state and geographical comparisons,
turning raw telemetry into an operational control centre.

- **Critical Alerts (Anomalies):** Isolates readings that breach safety
  thresholds, such as extreme pH or high turbidity, so hazardous conditions are
  visible by time and station.
- **Latest Current Readings:** Gauge cards show the most recent payload.
  `COALESCE` supplies a safe fallback for missing values, allowing occasional
  dropped sensor fields without breaking the visual interface.
- **Average Metrics by Station:** A bar chart compares average pH and turbidity
  by location, identifying stations with higher baseline contamination.
- **Streamflow & Conductance:** An area chart relates water volume to dissolved
  particles over time, helping indicate dilution or new pollutant introduction.
- **Temperature & Dissolved Oxygen:** A line chart establishes baseline aquatic
  health conditions for the selected time range.

### Dashboard 2: Advanced Trend Analysis & Diagnostics

![Advanced Trend Analysis & Diagnostics dashboard](docs/images/dashboard-2.png)

This dashboard uses time bucketing and window functions to reduce noisy IoT
signals and expose physical environmental relationships.

- **pH Volatility Profile:** Displays minimum, average, and maximum pH
  side-by-side. A normal average with wide extremes signals chemical
  instability.
- **Hourly Contaminant Averages:** Groups five-minute telemetry into hourly
  buckets, exposing macro-trends and recurring dissolved-pollutant spikes.
- **Suffocation Correlation:** A dual-axis comparison of temperature and
  dissolved oxygen demonstrates their inverse relationship and highlights
  periods of aquatic-life risk.
- **The Sediment Kick-Up:** Overlays streamflow and turbidity to test whether
  fast water movement is disturbing sediment and reducing clarity.
- **The Flash Flood Indicator:** Uses `LAG()` to calculate the change in flow
  between readings. Positive spikes indicate surges; negative spikes indicate
  sudden drops or possible upstream blockages.

### Panel query library

[`sql/dashboard-queries.sql`](sql/dashboard-queries.sql) contains the exact
SQL for all ten panels, grouped by dashboard and annotated with purpose and
recommended visualization. Grafana expands `$__timeFilter(event_timestamp)`
to the time range selected in the dashboard.

### Public dashboard

[Open Dashboard 1: Operational Monitoring & Alerts](https://sereneiris1223.grafana.net/public-dashboards/f9fb0a33a28f43ac878b03267ebce7d3)

[Open Dashboard 2: Advanced Trend Analysis & Diagnostics](https://sereneiris1223.grafana.net/public-dashboards/b51107f651d8479b9544615dd2edd2f5)

## Local development and deployment

### Prerequisites

- Python 3.12
- AWS CLI configured for `ap-south-1`
- A Supabase project with the schema applied
- A Grafana Cloud account or self-hosted Grafana instance

### Install dependencies

```cmd
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Environment variables

| Variable | Used by | Purpose |
|---|---|---|
| `SQS_QUEUE_URL` | Generator | Queue destination for telemetry batches. |
| `AWS_REGION` | Generator | AWS region for the SQS client. |
| `SUPABASE_DB_URI` | Processor | Transaction-pooler connection URI. |
| `SUPABASE_URL` | Processor and cleanup | Supabase project URL. |
| `SUPABASE_SERVICE_KEY` | Processor and cleanup | Server-side Storage API credential. |

Copy `cleanup_env.example.json` to `cleanup_env.json` before deploying the
cleanup Lambda. Keep all live environment files out of version control.

### Generate a test batch

```cmd
python src\generator.py
```

### Deploy the processor

```cmd
deploy_processor.bat
```

The deployment script builds the dependency package, publishes the processor
ZIP, waits for Lambda to finish updating, and applies `update_env.json`.

### Verify pipeline health

```cmd
aws logs tail /aws/lambda/water-quality-processor --region ap-south-1 --since 10m
```

```sql
SELECT
  count(*) AS readings,
  max(ingested_at) AS latest_ingestion
FROM water_quality_readings;
```

## Operations and security

- Store credentials only in ignored environment files or a managed secret
  store. Rotate a password or service key immediately if it is exposed.
- Copy the Supabase pooler hostname from the Connect dialog; do not construct
  it from a cloud region name.
- URL-encode reserved characters only when placing a password in a database
  URI. Grafana's password field expects the plain password.
- The cleanup Lambda deliberately receives no database URI because it only
  manages Supabase Storage.
- Failed processor messages land in `water-quality-dlq` after the configured
  SQS retry threshold. Redrive them only after the processor is healthy.
- The processor currently disables TLS certificate and hostname verification
  for the pooler. Replace that temporary workaround with CA validation before
  a production deployment.

## Scalability and scenarios

### Scaling path

```mermaid
flowchart LR
    LOW[Current: low to moderate load] --> MED[Higher message volume]
    MED --> HIGH[High-throughput production workload]

    LOW --- L1[One-message SQS batches\nPer-row inserts\nSingle dashboard]
    MED --- L2[Increase Lambda memory\nTune SQS concurrency\nBatch database writes]
    HIGH --- L3[Use bulk COPY or batched inserts\nPartition time-series tables\nAdd queue alarms and DLQ redrive automation]
```

| Scenario | Expected behavior | Recommended evolution |
|---|---|---|
| Normal station telemetry | One 125-reading batch is processed in a single Lambda invocation. | Keep SQS batch size at one and monitor duration. |
| Short traffic burst | SQS stores backlog while Lambda scales consumers. | Increase event-source maximum concurrency only after validating Supabase connection limits. |
| Duplicate SQS delivery | Existing event and batch keys are ignored. | Retain the current idempotent constraints and audit table. |
| Slow database writes | Lambda duration increases because inserts are per row. | Move to `executemany`, multi-row inserts, or PostgreSQL `COPY`. |
| Millions of readings | Retention deletes become more expensive and dashboards scan more data. | Partition by event time, retain rollups, and add summary tables or materialized views. |
| Dashboard growth | More users open concurrent Grafana panels. | Create a dedicated read-only Grafana role and tune session-pooler connection limits. |
| Long-term analytics | Five-day operational retention is insufficient. | Export raw files to low-cost object storage and build an analytical warehouse or lakehouse layer. |

The current architecture is intentionally optimized for a small operational
telemetry workload. SQS provides the buffer for burst absorption, while the
database and Storage layers remain independently scalable. The next major
performance improvement is replacing per-row database calls with bulk writes;
that reduces latency, lowers Lambda cost, and enables higher SQS concurrency.
