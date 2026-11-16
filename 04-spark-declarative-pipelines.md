
# Databricks - Introduction to Spark Declarative Pipelines

## 1. Overview

Apache Spark Declarative Pipelines (SDP) is a Databricks framework for building and managing reliable **batch and streaming ETL pipelines** using SQL or Python.

**Declarative approach:** We define what data we want to produce, while Databricks manages dependencies, execution planning, and much of the operational complexity.

### Main Benefits
- Simplifies pipeline development.
- Automatically manages table dependencies.
- Supports incremental data processing.
- Provides built-in reliability and optimizations.
- Reduces manual checkpointing and retry management.
- Supports batch and streaming workloads.

---

## 2. Databricks Data Engineering Architecture

Databricks Lakeflow consists of three main components:

| Component | Purpose |
|---|---|
| Lakeflow Connect | Ingest data from cloud storage, databases, and applications |
| Spark Declarative Pipelines | Transform data using SQL or Python |
| Lakeflow Jobs | Schedule and orchestrate workflows |

**Supporting technologies:**

- **Apache Spark:** Distributed data processing engine.
- **Unity Catalog:** Centralized governance and access control.
- **Delta Lake:** Reliable storage with ACID transactions.
- **Parquet / Iceberg:** Additional storage formats and table technologies.

### Example Workflow

```text
Data Sources (S3, ADLS, Databases)
                 |
                 v
          Lakeflow Connect
                 |
                 v
      Spark Declarative Pipelines
                 |
          Bronze → Silver → Gold
                 |
                 v
           Power BI / AI
```

Lakeflow Jobs can schedule and orchestrate the pipeline.

---

## 3. Why Spark Declarative Pipelines?

Traditional data pipelines require engineers to manage:

- Dependencies between processing stages.
- Failures, retries, and recovery.
- Data quality checks.
- Checkpoints and historical backfills.
- Infrastructure and scaling.
- Batch and streaming workloads.

SDP reduces this operational work, allowing engineers to focus on transformation logic.

### Declarative vs Traditional Approach

| Traditional | Declarative |
|---|---|
| Manually coordinate processing steps | Define the desired output tables |
| Manage dependencies explicitly | Framework identifies table dependencies |
| More operational code | Less boilerplate code |
| Manual performance tuning | Built-in optimizations assist processing |

**Note:** SDP simplifies operations but does not eliminate the need for monitoring or configuration.

---

## 4. Batch vs Streaming Processing

### Batch Processing
Processes data in groups, usually at scheduled intervals.

**Example:** Process daily sales files every night.

### Streaming Processing
Processes newly arriving data incrementally.

**Example:** Process new customer orders throughout the day.

| Batch | Streaming |
|---|---|
| Scheduled processing | Continuous or triggered processing |
| Data processed in groups | New data processed incrementally |
| Daily reporting | Frequent or near-real-time updates |

**Important:** Streaming Tables can also run in scheduled pipelines.

---

## 5. Medallion Architecture Using SDP

A common architecture uses Streaming Tables for Bronze and Silver, followed by a Materialized View for Gold.

```text
            RAW DATA SOURCE
                   |
                   v
        BRONZE STREAMING TABLE
           Raw data ingestion
                   |
                   v
        SILVER STREAMING TABLE
        Cleaning and transformation
                   |
                   v
         GOLD MATERIALIZED VIEW
          Business aggregations
                   |
                   v
          Power BI / Analytics
```

### Bronze Layer
- Ingest raw records.
- Preserve source information.
- Support incremental ingestion.

### Silver Layer
- Clean invalid records.
- Convert data types.
- Apply transformation rules.

### Gold Layer
- Calculate KPIs and aggregations.
- Prepare data for dashboards and analytics.

---

## 6. Streaming Tables vs Materialized Views

### Streaming Table
A table maintained using streaming processing. It is useful for incrementally ingesting and transforming newly available data.

### Materialized View
A stored query result that is maintained through refresh operations. It is commonly used for aggregations and reporting.

| Feature | Streaming Table | Materialized View |
|---|---|---|
| Main purpose | Incremental processing | Maintain query results |
| Common layer | Bronze / Silver | Gold |
| Typical use | Ingestion and cleaning | Aggregation and reporting |
| Processing | New streaming input | Incremental or full refresh |

**Important:** Materialized Views do not always refresh incrementally. The refresh method depends on the query and supported optimizations.

---

## 7. Incremental Processing

Incremental processing means processing newly available data without unnecessarily reprocessing existing records.

### Run 1 - Initial Data

File 1 contains 1,000 sales records.

1. Bronze ingests 1,000 records.
2. Silver cleans and transforms the records.
3. Gold calculates revenue aggregations.

### Run 2 - New Data

File 2 contains 200 additional sales records.

1. Bronze ingests the 200 new records.
2. Silver transforms the new records.
3. Gold refreshes its results using the updated data.

**Final result:** 1,200 records are available across the ingested dataset, assuming no records were filtered out.

### Why Is Incremental Processing Useful?
- Avoids unnecessary processing.
- Improves efficiency.
- Supports frequent updates.
- Can reduce compute costs.

### Checkpointing

A **checkpoint** tracks streaming processing progress and helps the pipeline resume after failures.

Example:

```text
Run 1 → Process File 1 → Save progress
Run 2 → Process File 2 → Update progress
```

Checkpointing does not automatically remove duplicate business records.

---

## 8. Practical SQL Example

The following example defines a Bronze → Silver → Gold pipeline.

*Assumes a configured Spark Declarative Pipeline and an existing Unity Catalog volume.*

### Bronze - Streaming Table

```sql
CREATE OR REFRESH STREAMING TABLE bronze_orders
AS
SELECT *
FROM STREAM read_files(
    '/Volumes/retail/raw/orders/',
    format => 'csv',
    header => true
);
```

**Purpose:** Incrementally ingest CSV files.

### Silver - Streaming Table

```sql
CREATE OR REFRESH STREAMING TABLE silver_orders
AS
SELECT
    CAST(order_id AS INT) AS order_id,
    state,
    CAST(amount AS DOUBLE) AS amount
FROM STREAM(bronze_orders)
WHERE order_id IS NOT NULL
  AND amount IS NOT NULL;
```

**Purpose:** Clean records and convert data types.

### Gold - Materialized View

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_revenue
AS
SELECT
    state,
    SUM(amount) AS total_revenue
FROM silver_orders
GROUP BY state;
```

**Purpose:** Calculate total revenue by state.

The framework determines dependencies from these table definitions.

---

## 9. Creating a Spark Declarative Pipeline

The lecture introduces two ways to create a pipeline:

**Method 1 - Workspace Menu**
1. Open the Databricks workspace.
2. Select the options menu (`...`).
3. Choose **Create → ETL Pipeline**.

**Method 2 - Jobs & Pipelines**
1. Open **Jobs & Pipelines**.
2. Select the pipeline creation option.
3. Configure the pipeline.

Detailed setup is covered in the next practical lecture.

---

## 10. Interview Revision Questions

**Q1. What is Spark Declarative Pipelines?**  
A framework for building and managing batch and streaming data pipelines using SQL or Python.

**Q2. What is declarative programming?**  
Defining the required output while the framework manages much of the execution.

**Q3. Streaming Table vs Materialized View?**  
Streaming Tables process new data incrementally. Materialized Views maintain stored query results.

**Q4. What is incremental processing?**  
Processing new data without unnecessarily reprocessing previously handled data.

**Q5. What is checkpointing?**  
Tracking streaming progress and state to support recovery and incremental processing.

**Q6. What is the role of Lakeflow Jobs?**  
Scheduling and orchestrating tasks and pipelines.

---

## 11. Quick Revision

| Term | Remember |
|---|---|
| Lakeflow Connect | Ingest |
| Spark Declarative Pipelines | Transform |
| Lakeflow Jobs | Orchestrate |
| Streaming Table | Process new data |
| Materialized View | Maintain query results |
| Incremental Processing | Avoid unnecessary reprocessing |
| Checkpoint | Track streaming progress |
| Bronze | Raw data |
| Silver | Cleaned data |
| Gold | Business-ready data |

### Key Takeaway

**Spark Declarative Pipelines simplifies reliable data engineering by allowing developers to define SQL/Python transformations while Databricks manages dependencies and much of the processing infrastructure.**

---

## Official References

- [Databricks Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/)
- [Pipeline Transformations](https://docs.databricks.com/aws/en/ldp/transform)
- [Databricks Documentation](https://docs.databricks.com/)
