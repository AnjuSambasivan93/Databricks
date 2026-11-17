
# Databricks - Simplified Pipeline Development and Settings

**Course:** Build Data Pipelines with Apache Spark Declarative Pipelines (SDP)

## 1. Overview

Databricks provides a **Multi-File Editor** to develop, organize, debug, and validate data pipelines using SQL or Python.

**Main idea:** Write pipeline code in separate files and use pipeline settings to control how the pipeline runs.

---

## 2. Multi-File Editor

The Multi-File Editor allows developers to manage multiple SQL and Python files within one pipeline project.

### Example Project Structure

```text
retail_pipeline/
├── orders_bronze.sql
├── orders_silver.sql
├── gold_orders.sql
└── customers.py
```

### Key Features

- **Pipeline Asset Browser:** Navigate pipeline files.
- **Code Editor:** Write SQL and Python transformations.
- **DAG Visualization:** View dependencies between tables.
- **Data Preview:** Inspect pipeline data.
- **Execution Insights:** Investigate pipeline runs.
- **Validation:** Check pipeline definitions without a full run.

**Benefit:** Easier development, debugging, collaboration, and maintenance.

---

## 3. Common Pipeline Settings

Pipeline settings control how a pipeline is configured and executed.

Access them using the **gear icon** in the pipeline editor.

| Setting | Purpose |
|---|---|
| Compute | Select processing resources |
| Code Assets | Specify SQL/Python files |
| Configuration | Define reusable parameters |

### A. Compute

Compute provides the resources required to execute the pipeline.

**Serverless Compute (Recommended)**
- Databricks manages the infrastructure.
- Less manual configuration.
- Optimized for cost and performance.
- Supports incremental refresh for eligible Materialized Views.
- Uses cost-based optimization.

**Classic Compute**
- Uses configured compute resources.
- Requires more infrastructure configuration.

**Remember:** Compute = Resources that execute pipeline code.

---

## 4. Code Assets

Code Assets define which files are included in the pipeline.

### Pipeline Root Folder

The main folder containing pipeline files.

Example:

```text
retail_pipeline/
├── bronze/
│   └── orders.sql
├── silver/
│   └── orders.sql
└── gold/
    └── sales.sql
```

The root folder can be Git-backed for version control.

### Source Code Section

Allows developers to select individual files or subfolders containing:

- SQL files (`.sql`)
- Python scripts (`.py`)
- Notebooks

**Remember:** Code Assets = Files used by the pipeline.

---

## 5. Configuration Parameters

Configuration parameters are reusable **key-value pairs**.

They help avoid hardcoding values inside SQL or Python files.

### Example Configuration

| Key | Value |
|---|---|
| `source` | `/Volumes/dbacademy/ops/dbacademy_warehouse` |

### SQL Example

```sql
CREATE OR REFRESH STREAMING TABLE orders_bronze AS
SELECT *
FROM STREAM read_files(
    "${source}/orders",
    format => 'JSON'
);
```

### How It Works

`${source}` references the configured value.

The resulting source path is:

```text
/Volumes/dbacademy/ops/dbacademy_warehouse/orders
```

### Benefits

1. Avoids hardcoding file paths.
2. Reuses values across multiple files.
3. Makes pipelines easier to maintain.
4. Supports different development and production configurations.

**Remember:** Configuration = Reusable key-value parameters.

---

## 6. Additional Pipeline Settings

| Setting | Purpose |
|---|---|
| Trigger Modes | Control how pipeline updates are initiated |
| Budget | Configure cost controls |
| Advanced Settings | Fine-tune pipeline behaviour |

These settings are introduced in the lecture and explored further in practical demonstrations.

---

## 7. How Everything Works Together

```text
Multi-File Editor
       |
       v
SQL / Python Files
       |
       v
Pipeline Settings
  ├── Compute
  ├── Code Assets
  └── Configuration
       |
       v
Spark Declarative Pipeline
       |
       v
Bronze → Silver → Gold
       |
       v
Analytics / Reporting
```

**Example:**

A retail company develops SQL transformations in separate files, selects Serverless Compute, includes the files through Code Assets, and uses a `source` parameter to locate JSON files.

---

## 8. Interview Revision 

### Q1. What is the Multi-File Editor?

A Databricks development environment where we can organize, write, debug, and validate multiple SQL and Python files for a pipeline.

### Q2. What is Serverless Compute?

A compute option where Databricks manages the underlying infrastructure, allowing developers to focus on their pipeline code.

### Q3. What are Code Assets?

The SQL files, Python scripts, and notebooks included in a pipeline.

### Q4. What are Configuration Parameters?

Reusable key-value pairs used to configure a pipeline without repeatedly changing its code.

**Example:** Store a source folder path in a parameter called `source`.

### Q5. What does `${source}` mean?

It references the value of the pipeline configuration parameter named `source`.

### Q6. What is the difference between Compute and Code Assets?

- **Compute:** Resources that execute the pipeline.
- **Code Assets:** Files containing the pipeline logic.

### Q7. What is DAG Visualization?

A graphical representation of dependencies between pipeline datasets.

---

## 9. Quick Revision

| Concept | Remember |
|---|---|
| Multi-File Editor | Develop and debug multiple files |
| Pipeline Asset Browser | Navigate pipeline files |
| Serverless Compute | Databricks manages infrastructure |
| Classic Compute | Configure compute resources |
| Pipeline Root Folder | Main project directory |
| Code Assets | Pipeline source files |
| Configuration | Key-value parameters |
| `${source}` | Reference a configuration value |
| DAG | Visualize dataset dependencies |
| Validation | Check definitions before full execution |

### Key Takeaway

**The Multi-File Editor simplifies pipeline development, while pipeline settings control compute resources, source files, and reusable configuration values.**

---

## Official References

- [Databricks Multi-File Editor](https://docs.databricks.com/aws/en/dlt/dlt-multi-file-editor)
- [Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/)
