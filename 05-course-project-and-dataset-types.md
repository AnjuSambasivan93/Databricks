
# Databricks - Course Project and Dataset Types

**Course:** Build Data Pipelines with Apache Spark Declarative Pipelines (SDP)

## 1. Course Project Overview

The project builds a retail data pipeline using JSON files stored in cloud storage.

It contains three flows:

| Flow | Purpose |
|---|---|
| Orders Flow | Ingest orders and calculate daily order counts |
| Status Flow | Process order statuses and join with orders |
| Customers Flow | Clean customer data and track changes using CDC |

### Project Architecture

```text
ORDERS FLOW
Orders JSON
    ↓
orders_bronze (Streaming Table)
    ↓
orders_silver (Streaming Table)
    ↓
gold_orders_by_date (Materialized View)

STATUS FLOW
Status JSON
    ↓
status_bronze (Streaming Table)
    ↓
status_silver
    ↓
Join with orders_silver
    ↓
full_order_info_gold (Materialized View)
    ├── cancelled_orders (Materialized View)
    └── delivered_orders (Materialized View)

CUSTOMERS FLOW
Customers JSON
    ↓
customers_bronze
    ↓
customers_bronze_clean
    ↓
type1_customers_silver (CDC Type 1)
```

---

## 2. Dataset Types in SDP

Spark Declarative Pipelines supports three main dataset types.

| Dataset | Purpose | Stores Data/Results? |
|---|---|---|
| Streaming Table (ST) | Incremental data processing | Yes |
| Materialized View (MV) | Maintain calculated query results | Yes |
| View | Virtual table based on SQL | No |

### Streaming Table

- Processes newly arriving data incrementally.
- Commonly used for ingestion and transformations.
- Supports scheduled and continuous processing.
- Uses `CREATE OR REFRESH STREAMING TABLE`.
- Uses `STREAM` for streaming reads.

### Materialized View

- Stores the result of a SQL query.
- Commonly used for aggregations and reporting.
- Refreshes when the pipeline updates it.
- Can use incremental or full refresh.
- Uses `CREATE OR REFRESH MATERIALIZED VIEW`.

### View

- Stores a SQL query definition, not physical query results.
- Useful for reusable queries and intermediate logic.
- Two types: Temporary View and Standard View.

---

## 3. Bronze Streaming Table - SQL

```sql
CREATE OR REFRESH STREAMING TABLE 1_bronze_db.orders_bronze AS
SELECT
    *,
    current_timestamp() AS processing_time,
    _metadata.file_name AS source_file
FROM STREAM read_files(
    "{{ source_path }}/orders",
    format => 'JSON'
);
```

### Explanation

- `CREATE OR REFRESH STREAMING TABLE`: Defines a Streaming Table.
- `SELECT *`: Reads all source columns.
- `current_timestamp()`: Adds processing time.
- `_metadata.file_name`: Records the source filename.
- `STREAM`: Enables streaming semantics.
- `read_files()`: Reads source files using Auto Loader.
- `format => 'JSON'`: Specifies the file format.

**Important:** Auto Loader tracks processed files to support reliable incremental ingestion.

---

## 4. Silver Streaming Table - SQL

```sql
CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver AS
SELECT
    order_id,
    timestamp(order_timestamp) AS order_timestamp,
    customer_id,
    notifications
FROM STREAM 1_bronze_db.orders_bronze;
```

### Explanation

- Reads new records from `orders_bronze`.
- Selects required columns.
- Converts `order_timestamp` to a timestamp.
- Incrementally processes newly available data.

**Key point:** `FROM STREAM` is used when reading a source with streaming semantics.

---

## 5. Gold Materialized View - SQL

```sql
CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db.gold_orders_by_date AS
SELECT
    date(order_timestamp) AS order_date,
    count(*) AS total_daily_orders
FROM 2_silver_db.orders_silver
GROUP BY date(order_timestamp);
```

### Explanation

- `date()`: Extracts the order date.
- `count(*)`: Counts orders.
- `GROUP BY`: Groups orders by date.
- Materialized View stores the calculated result.

Example:

| order_date | total_daily_orders |
|---|---:|
| 2026-11-14 | 150 |
| 2026-11-15 | 220 |

**Important:** Materialized Views do not use `STREAM` in this example.

On supported Serverless compute, a cost-based optimizer can choose incremental refresh or full recomputation.

---

## 6. Views and Temporary Views

### Temporary View

```sql
CREATE TEMPORARY VIEW orders_active AS
SELECT *
FROM 2_silver_db.orders_silver
WHERE notifications = 'Y';
```

- Used for intermediate pipeline logic.
- Pipeline-scoped.
- Not registered as a persistent Unity Catalog object.

### Standard View

```sql
CREATE VIEW 3_gold_db.orders_active_view AS
SELECT *
FROM 2_silver_db.orders_silver
WHERE notifications = 'Y';
```

- Registered in Unity Catalog.
- Persists beyond the pipeline run.
- Does not store physical query results.

| Temporary View | Standard View |
|---|---|
| Pipeline-scoped | Persistent |
| Not registered in Unity Catalog | Registered in Unity Catalog |
| Internal intermediate queries | Reusable queries |

---

## 7. Customers Flow - CDC Type 1

**CDC (Change Data Capture)** identifies changes in source data, such as inserts, updates, and deletes.

The Customers Flow uses CDC to maintain:

`type1_customers_silver`

### SCD Type 1

**SCD (Slowly Changing Dimension):** Defines how changes are stored in a dimension table.
SCD Type 1 overwrites old values with the latest values.

Example:

Before:

| customer_id | city |
|---|---|
| 501 | Auckland |

After:

| customer_id | city |
|---|---|
| 501 | Christchurch |

The old city is replaced rather than preserved as a historical version.

**Use case:** Maintain the latest customer information.

---

## 8. Automatic Dependency Resolution

SDP automatically determines dependencies between datasets.

```text
orders_bronze
      ↓
orders_silver
      ↓
gold_orders_by_date
```

- Silver depends on Bronze.
- Gold depends on Silver.
- Databricks builds the Declarative Pipeline Graph.
- Developers do not need to manually arrange SQL definitions in execution order.

---

## 9. DLT vs SDP Syntax

| Older DLT Syntax | Modern SDP Syntax |
|---|---|
| `STREAMING LIVE TABLE` | `STREAMING TABLE` |
| `LIVE TABLE` | `MATERIALIZED VIEW` |
| `LIVE VIEW` | `VIEW` |
| `TEMPORARY LIVE VIEW` | `TEMPORARY VIEW` |

Older syntax remains supported for backward compatibility.

---

## 10. Interview Revision

**Q1. What is a Streaming Table?**  
A table designed for incremental data processing.

**Q2. What is a Materialized View?**  
A stored SQL query result maintained through refresh operations.

**Q3. Streaming Table vs Materialized View?**  
Streaming Tables process new data incrementally. Materialized Views maintain calculated results.

**Q4. What is a Temporary View?**  
A pipeline-scoped virtual table used for intermediate queries.

**Q5. What is CDC Type 1?**  
A method that overwrites existing values with the latest data.

**Q6. What is automatic dependency resolution?**  
Databricks identifies dependencies between tables and determines the processing order.

---

## 11. Quick Revision

| Concept | Remember |
|---|---|
| Orders Flow | Daily order aggregation |
| Status Flow | Order status processing and joins |
| Customers Flow | Customer updates using CDC |
| Streaming Table | Process new data |
| Materialized View | Maintain calculated results |
| Temporary View | Pipeline-scoped virtual query |
| Standard View | Persistent virtual query |
| Auto Loader | Incremental file ingestion |
| SCD Type 1 | Overwrite old values |
| Pipeline Graph | Automatic dependency management |

### Key Takeaway

**Spark Declarative Pipelines simplifies data engineering by combining incremental ingestion, SQL transformations, materialized aggregations, CDC, and automatic dependency management.**

---

## Official References

- [Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/)
- [Databricks Academy - Current Lesson](https://customer-academy.databricks.com/learn/learning-plans/10/data-engineer-learning-plan/courses/2971/build-data-pipelines-with-apache-spark-declarative-pipelines/lessons/63328:4384/course-project-and-dataset-types-overview)
