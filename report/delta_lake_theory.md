# Delta Lake — Theory Guide

A consolidated theory reference distilled from the Delta Lake course notebooks (`work/delta/*.ipynb`). Hands-on exercises, environment setup, and sample-code walkthroughs have been stripped out — only the conceptual/theoretical material remains, reorganized into a single hierarchical document.

## Table of Contents

1. [Introduction to Delta Lake & the Delta Log](#1-introduction-to-delta-lake--the-delta-log)
2. [Creating & Managing Delta Tables](#2-creating--managing-delta-tables)
3. [Reading & Writing Data — Write Modes, Partitioning, Data Skipping](#3-reading--writing-data--write-modes-partitioning-data-skipping)
4. [DML — UPDATE, DELETE, MERGE (Upsert) & SCD Type 2](#4-dml--update-delete-merge-upsert--scd-type-2)
5. [Time Travel & Version History](#5-time-travel--version-history)
6. [Schema Enforcement, Schema Evolution & Constraints](#6-schema-enforcement-schema-evolution--constraints)
7. [Performance Optimization — OPTIMIZE, Z-ORDER & the Small-File Problem](#7-performance-optimization--optimize-z-order--the-small-file-problem)
8. [VACUUM — Cleaning Up Old Files & Data Lifecycle Management](#8-vacuum--cleaning-up-old-files--data-lifecycle-management)
9. [Change Data Feed (CDF)](#9-change-data-feed-cdf)
10. [Structured Streaming with Delta Lake](#10-structured-streaming-with-delta-lake)
11. [Concurrency Control & Transaction Conflicts](#11-concurrency-control--transaction-conflicts)
12. [Cloning Tables (SHALLOW/DEEP CLONE) & Synthesis](#12-cloning-tables-shallowdeep-clone--synthesis)

---

## 1. Introduction to Delta Lake & the Delta Log

**Goals:** understand what Delta Lake is and what problem it solves compared to a plain Parquet data lake; understand the directory layout of a Delta table and the **transaction log** mechanism (`_delta_log`); understand the 4 **ACID** properties Delta Lake provides on top of object storage.

### 1.1. What is Delta Lake?

**Delta Lake** is an open-source storage layer built on top of **Parquet** files, adding:

- **ACID transactions**: every write operation (write/update/delete/merge) is atomic — it either fully succeeds or changes nothing. There is never a "partially written" state.
- **Schema enforcement & evolution**: tables have a well-defined schema that rejects wrongly-typed data, while still allowing controlled schema expansion.
- **Time travel**: read back any past version of the table.
- **Unified batch & streaming**: a single Delta table can be both a streaming sink and a batch source.
- **Scalable metadata**: metadata (file listings, statistics...) is stored in a form Spark can process as a job, instead of having to `list` millions of files on object storage (which is very slow on S3/MinIO).

#### The problem with plain Parquet/Hive data lakes

| Problem | Plain Parquet/Hive | Delta Lake |
|---|---|---|
| Overwrite fails midway | Corrupt data, half-written files get read | Transaction log guarantees all-or-nothing |
| Multiple concurrent writers | Race conditions, data loss | Optimistic concurrency control |
| Reading while writing | May read inconsistent data | Snapshot isolation — always reads one consistent version |
| Changing schema | Manual, easily breaks downstream consumers | Controlled schema enforcement + evolution |
| "Rolling back" old data | Not possible, must build your own backups | `VERSION AS OF` / `TIMESTAMP AS OF` |

This is the foundation of the **Lakehouse** architecture: combining the low cost and scalability of a data lake with the transactional guarantees of a data warehouse.

### 1.2. Physical structure of a Delta table

```
s3a://data-platform/managed/bai01.db/events/
├── _delta_log/
│   ├── 00000000000000000000.json   # commit 0 (CREATE / first write)
│   ├── 00000000000000000001.json   # commit 1
│   ├── 00000000000000000002.json   # commit 2
│   ├── ...
│   └── _last_checkpoint            # (appears once a checkpoint exists)
├── part-00000-....snappy.parquet   # actual data file
├── part-00001-....snappy.parquet
└── ...
```

- **The actual data** is always stored as standard **Parquet** files — Delta does not change the data file format.
- **`_delta_log/`** contains JSON files — each file is **one transaction (commit)**, numbered sequentially starting at `0`.
- Each commit JSON contains "actions" such as:
  - `add` — adds a Parquet file to the table (with statistics: min/max, row count, null count...)
  - `remove` — marks a file as no longer belonging to the table (a tombstone; **not physically deleted immediately**)
  - `metaData` — schema, partition columns, table properties
  - `commitInfo` — who did what operation and when (INSERT/UPDATE/DELETE/MERGE/OPTIMIZE...)
  - `protocol` — the minimum reader/writer version required to safely read/write the table

**The table's current state** is the aggregation (replay) of all actions from commit `0` up to the latest commit. This is precisely why Delta Lake can support *time travel*: it only needs to replay up to an older commit.

After a certain number of commits (10 by default), Spark automatically writes an additional **checkpoint** (`.checkpoint.parquet`) — a full snapshot of the state in Parquet form, so the entire JSON history doesn't need to be re-read every time the table is opened.

### 1.3. ACID in Delta Lake

- **Atomicity**: a write job produces exactly one new JSON commit file; if the job crashes midway, the JSON file is never written → it's as if nothing happened.
- **Consistency**: schema and constraints are validated before a commit is accepted.
- **Isolation**: achieved via **optimistic concurrency control** — each writer reads the current version, works independently, and then attempts to commit the next version; if it loses the race (a conflict), that writer automatically retries or raises an error (see Section 11).
- **Durability**: once a commit's JSON file has been successfully written to storage (S3/MinIO), the change is permanent.

---

## 2. Creating & Managing Delta Tables

**Goals:** distinguish between **managed** and **external** tables in Delta Lake; know the 3 ways to create a table: `CREATE TABLE` (SQL DDL), `DataFrameWriter.saveAsTable`, `DeltaTable.create` (Python API); know how to view/edit metadata and table properties.

### 2.1. Managed table vs External table

| | **Managed table** | **External table** |
|---|---|---|
| Location | Controlled by the metastore (`spark.sql.warehouse.dir/<db>.db/<table>`) | You specify it yourself via `LOCATION` |
| `DROP TABLE` | Deletes **both metadata and physical data** | Only deletes metadata; **data remains** on storage |
| When to use | A table fully "owned" by one pipeline/team | Data shared across multiple systems, or when you want manual control over the file lifecycle |

Both types register in the **Hive Metastore** (here, `thrift://hive-metastore:9083`), so both can be queried by `db.table` name — the only real difference is who is responsible for deleting the physical data.

### 2.2. Three ways to create a Delta table

1. **SQL DDL**:
```sql
CREATE TABLE db.tbl (id INT, name STRING) USING DELTA;                 -- managed
CREATE TABLE db.tbl (id INT, name STRING) USING DELTA LOCATION 's3a://...'; -- external
```
2. **DataFrameWriter**:
```python
df.write.format("delta").mode("overwrite").saveAsTable("db.tbl")        # managed
df.write.format("delta").mode("overwrite").save("s3a://bucket/path")    # path-based, not registered in the metastore
```
3. **The `DeltaTable` Python API** (`delta.tables.DeltaTable`) — used for programmatic operations (`.create()`, `.createIfNotExists()`), useful when a builder pattern is needed for complex constraints/generated columns.

A table can also exist **purely as a path** (`s3a://.../my_table`, written with `.save()`) without being registered in the Hive Metastore — it is still a valid Delta table, readable via `spark.read.format("delta").load(path)`, just without a `db.table` name to query with SQL.

---

## 3. Reading & Writing Data — Write Modes, Partitioning, Data Skipping

**Goals:** master the 4 write modes: `append`, `overwrite`, `errorifexists`, `ignore`; use `replaceWhere` for selective overwrite; understand how **partition pruning** and **data skipping** make Delta faster than plain Parquet.

### 3.1. Write modes

| Mode | Behavior when the table already exists |
|---|---|
| `append` | Adds new data, keeps existing data unchanged |
| `overwrite` | Deletes **all** existing data, replaces it with new data (history is still kept — time travel can still see the old version) |
| `errorifexists` (the default for `.save()`) | Throws an error if the table/path already exists |
| `ignore` | Does nothing if the table/path already exists |

#### Selective overwrite with `replaceWhere`

Instead of overwriting the entire table, you can overwrite **only the partitions/rows matching a condition**, leaving the rest untouched:

```python
(df_new.write.format("delta")
   .mode("overwrite")
   .option("replaceWhere", "order_date = '2024-01-02'")
   .saveAsTable("bai03.orders"))
```

This is a common way to "backfill" a single day's worth of data without touching other days — much faster than reading the whole table and overwriting it, and safer than a manual UPDATE/DELETE for large volumes.

> From Delta 2.x+, by default the **schema of the new data must be contained within** the data being replaced by `replaceWhere` (a safer dynamic partition overwrite).

### 3.2. Partition pruning & Data skipping

- **Partition pruning**: if a table is `PARTITIONED BY (country)`, the physical data is split into subdirectories such as `country=VN/`, `country=US/`, etc. When a query has `WHERE country = 'VN'`, Spark **reads only that directory**, skipping all other partitions entirely — without even needing to open a file to know.
- **Data skipping**: for columns that are **not** partition columns, every commit in `_delta_log` still stores **min/max statistics** for the first 32 columns (by default) of each Parquet file. When a query has a filter (`WHERE price > 100`), Delta reads the statistics in the log to exclude files that definitely can't match — **without opening the Parquet file**. This is why Delta Lake is faster than plain Parquet even though it uses the same file format.

Rule of thumb for choosing a partition column: use a column with **low-to-medium cardinality** (date, country...) that **frequently appears in WHERE clauses**. Partitioning by a very high-cardinality column (e.g. `user_id`) creates millions of tiny directories — counterproductive (see Section 7 on the small-file problem).

---

## 4. DML — UPDATE, DELETE, MERGE (Upsert) & SCD Type 2

**Goals:** perform `UPDATE` and `DELETE` directly on a Delta table (something plain Parquet/Hive doesn't support well); master `MERGE INTO` for upserts (insert-or-update), including `WHEN NOT MATCHED BY SOURCE`; understand and implement the **Slowly Changing Dimension Type 2 (SCD2)** pattern using `MERGE`.

### 4.1. UPDATE / DELETE

Unlike plain Parquet/Hive (which requires reading everything, filtering, then rewriting the whole table), Delta Lake supports `UPDATE`/`DELETE` like a real relational table:

```sql
UPDATE db.tbl SET status = 'shipped' WHERE order_id = 5;
DELETE FROM db.tbl WHERE order_date < '2020-01-01';
```

Underlying mechanism: Delta **never edits a Parquet file in place** (files are immutable). For every Parquet file containing at least one matching row, Delta reads that file, writes out a **new Parquet file** with the updated/deleted data applied, then records in the transaction log: `remove` the old file + `add` the new one. Files entirely unaffected by the condition are left untouched — not read or rewritten.

There is also an equivalent Python API via `DeltaTable`:
```python
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "db.tbl")
dt.update(condition="order_id = 5", set={"status": "'shipped'"})
dt.delete("order_date < '2020-01-01'")
```

### 4.2. MERGE INTO (Upsert)

`MERGE` matches a target table against a new source of data based on a condition, then applies different actions to different groups:

```sql
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE   -- (optional) delete records present in target but absent from source
```

- `WHEN MATCHED` — a record matches on both sides → can `UPDATE` (either `SET *` or specific columns) or `DELETE`.
- `WHEN NOT MATCHED [BY TARGET]` — a record exists only in the source → `INSERT`.
- `WHEN NOT MATCHED BY SOURCE` — a record exists only in the target (Delta 2.3+) → typically used to `DELETE` (synchronizing against a complete "source of truth").
- Additional conditions can be added: `WHEN MATCHED AND s.deleted = true THEN DELETE`.
- Multiple `WHEN MATCHED`/`WHEN NOT MATCHED` clauses are evaluated **in declared order**; the first clause whose condition holds is applied.

This is the central tool for building **incremental pipelines** (CDC ingestion, deduplication, upserts from an OLTP source...).

### 4.3. SCD Type 2 via MERGE

SCD2: when an attribute of a dimension changes, **the old record is kept** (marked as expired) and **a new record is added**, rather than overwriting in place. This requires columns such as `effective_date`, `end_date`, `is_current`. The standard approach: conceptually a 2-step `MERGE`, implemented by **unioning the source with a special `merge_key` flag** so that both "closing" the old record and "opening" the new one happen in a single `MERGE` statement.

---

## 5. Time Travel & Version History

**Goals:** view a table's change history with `DESCRIBE HISTORY`; read a table at an older version/timestamp using `VERSION AS OF` / `TIMESTAMP AS OF`; restore a table to an older version with `RESTORE TABLE`; understand the relationship between time travel and log/data retention (related to `VACUUM`, Section 8).

### 5.1. `DESCRIBE HISTORY`

Each commit in `_delta_log` corresponds to one row in `DESCRIBE HISTORY`, containing: `version`, `timestamp`, `operation` (WRITE/UPDATE/DELETE/MERGE/OPTIMIZE/RESTORE...), `operationParameters`, `operationMetrics` (rows added/removed/updated), `readVersion`, `isolationLevel`, etc.

```sql
DESCRIBE HISTORY db.tbl;            -- full history
DESCRIBE HISTORY db.tbl LIMIT 5;    -- 5 most recent commits
```

### 5.2. Reading data at an older version/timestamp

```sql
SELECT * FROM db.tbl VERSION AS OF 3;
SELECT * FROM db.tbl TIMESTAMP AS OF '2024-01-15 10:00:00';
```
Or via DataFrameReader:
```python
spark.read.format("delta").option("versionAsOf", 3).table("db.tbl")
spark.read.format("delta").option("timestampAsOf", "2024-01-15").table("db.tbl")
```

Mechanism: Delta simply replays `_delta_log` from commit `0` up to commit `N` (the requested version) to determine exactly which set of Parquet files belonged to that version — **no data file needs to have been modified**, because old (`remove`d) files remain physically on storage until cleaned up by `VACUUM`.

### 5.3. `RESTORE TABLE`

```sql
RESTORE TABLE db.tbl TO VERSION AS OF 3;
RESTORE TABLE db.tbl TO TIMESTAMP AS OF '2024-01-15 10:00:00';
```

`RESTORE` **does not delete history** — it creates **a new commit** that brings the table back to the exact state of the old version (re-adding files that had been removed, and removing files that were added afterward). So after a restore, `DESCRIBE HISTORY` still shows every prior version **plus one new row with operation = RESTORE** — the restore operation itself can also be time-traveled to.

### 5.4. Practical limitations

- Time travel **depends on retention**: old data files only exist until `VACUUM` deletes them (7-day default retention); old JSON logs are governed by `delta.logRetentionDuration` (30-day default). After `VACUUM` runs, older versions referencing deleted files **can no longer be read**, even though the log entries still exist.
- The more versions kept, the more small leftover files accumulate until `VACUUM` runs — there's a balance to strike between audit/rollback needs and storage + file-listing performance costs.

---

## 6. Schema Enforcement, Schema Evolution & Constraints

**Goals:** understand that Delta Lake **rejects** writes with a mismatched schema by default (schema enforcement); allow controlled schema expansion via `mergeSchema` / `autoMerge`; use `ALTER TABLE` to add/modify/rename columns; add data constraints: `NOT NULL`, `CHECK` constraints, and **generated columns**.

### 6.1. Schema Enforcement

By default, when writing (`append`) into an existing table, Delta **checks the schema** of the DataFrame against the table's current schema:
- Missing columns, extra columns, or incompatible types (that can't be safely auto-cast) → **an error is thrown immediately**, nothing is written.
- This is a major difference from plain Parquet/Hive, which often "silently" ends up with inconsistent schemas across files.

### 6.2. Controlled Schema Evolution

To allow a DataFrame with **extra new columns** to be written, it must be explicitly declared:

```python
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("db.tbl")
```
or enabled at the session level: `spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")`.

For `overwrite`, to **fully replace the schema** (not just add columns), use:
```python
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("db.tbl")
```

`mergeSchema` only allows **safe** changes (adding new columns at the end, widening numeric types...); renaming/dropping columns or changing to an incompatible type still requires an explicit `ALTER TABLE` or `overwriteSchema`.

### 6.3. `ALTER TABLE`

```sql
ALTER TABLE db.tbl ADD COLUMNS (email STRING);
ALTER TABLE db.tbl ALTER COLUMN price TYPE DOUBLE;
ALTER TABLE db.tbl RENAME COLUMN name TO full_name;     -- requires column mapping mode
ALTER TABLE db.tbl DROP COLUMN old_col;                 -- requires column mapping mode
```
`RENAME COLUMN`/`DROP COLUMN` require **column mapping** to be enabled:
```sql
ALTER TABLE db.tbl SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
```

### 6.4. Constraints

```sql
ALTER TABLE db.tbl ADD CONSTRAINT positive_price CHECK (price > 0);
ALTER TABLE db.tbl CHANGE COLUMN id SET NOT NULL;
```
If any existing row already violates the constraint being added, the `ADD CONSTRAINT` command fails immediately. Once added, any `INSERT`/`UPDATE`/`MERGE` that would violate it is rejected.

### 6.5. Generated columns

A column **automatically computed** from other columns — a convenient way to derive a partition column from a timestamp without the application having to write it manually:

```sql
CREATE TABLE db.events (
    event_time TIMESTAMP,
    event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
) USING DELTA
PARTITIONED BY (event_date);
```
When writing, `event_date` doesn't need to be supplied — Delta computes it automatically. If a value is supplied that contradicts the formula, Delta raises an error.

---

## 7. Performance Optimization — OPTIMIZE, Z-ORDER & the Small-File Problem

**Goals:** understand the **small-file problem** — why writing repeatedly/streaming creates a large number of small files and why that's slow; use `OPTIMIZE` to compact (bin-pack) small files into larger ones; use `OPTIMIZE ... ZORDER BY` to reorganize data and improve data-skipping efficiency for columns that are frequently filtered together.

### 7.1. Small-file problem

Every `write`/`append`/streaming micro-batch creates a **new Parquet file**. With a frequent-write workload (streaming, multiple ingests per day), the number of small files grows quickly:

- Reading many small files incurs overhead from opening files and reading footers/metadata — much slower than reading fewer, larger files with the same total size.
- On object storage (S3/MinIO), every `list`/`open` is a network call — more files means slower performance and higher API-call costs.
- The Spark driver must hold metadata (paths, statistics) for **every file** in memory while planning a query.

### 7.2. `OPTIMIZE` (bin-packing / compaction)

```sql
OPTIMIZE db.tbl;                          -- compact the whole table
OPTIMIZE db.tbl WHERE order_date = '2024-01-01';   -- only one partition (reduces cost)
```

`OPTIMIZE` reads small files and merges them into files of a target size (~1GB by default, configurable via `spark.databricks.delta.optimize.maxFileSize` or the table property `delta.targetFileSize`), then commits `remove` (old files) + `add` (new files) — **the data itself doesn't change**, only how files are organized. Old versions remain time-travelable as normal (old files are only lost once `VACUUM` runs).

`OPTIMIZE` should be run periodically (not after every single write) — e.g. once a day for a continuously-ingesting table — to balance compaction cost against read benefits.

### 7.3. `ZORDER BY` — multi-dimensional data clustering

```sql
OPTIMIZE db.tbl ZORDER BY (customer_id, event_date);
```

Z-Ordering reorganizes data within files so that values that are **close together across multiple columns simultaneously** are stored physically close together (using a Z-order/Morton-code curve). Result: the min/max statistics of each file become much "tighter" along those columns → **significantly more effective data skipping** when queries filter on the Z-ordered columns — especially useful for columns that **aren't partition columns**, or when queries need to filter efficiently by **several columns at once** (partitioning only works well for 1-2 low-cardinality columns).

Choosing Z-order columns: prefer columns that **frequently appear in WHERE clauses** and have **high cardinality** (the opposite of what makes a good partition column) — e.g. `customer_id`, `device_id`. Avoid Z-ordering by too many columns (usually ≤ 3-4), since the benefit diminishes with each additional column.

### 7.4. `ANALYZE TABLE` — updating statistics

```sql
ANALYZE TABLE db.tbl COMPUTE STATISTICS FOR COLUMNS col1, col2;
```
Updates statistics used by the cost-based optimizer (CBO) to choose an appropriate join strategy — distinct from Delta's automatic min/max stats (used for data skipping); these are table-wide aggregate statistics (row count, NDV, histograms...).

---

## 8. VACUUM — Cleaning Up Old Files & Data Lifecycle Management

**Goals:** understand why Delta Lake **doesn't delete physical files immediately** on `DELETE`/`UPDATE`/`OPTIMIZE`/`overwrite`; use `VACUUM` to safely clean up, understanding the retention parameter and the `DRY RUN` flag; understand the trade-off between **longer retention** (safer for time travel and concurrent readers) and **storage cost**.

### 8.1. Why VACUUM is needed

Every "delete/overwrite" operation in Delta Lake (DELETE, UPDATE, MERGE, OPTIMIZE, overwrite) actually only marks old files as **`remove`** (a tombstone) in `_delta_log` — it does **not** immediately delete the physical Parquet file. Reasons:

1. **Time travel**: old files must still exist to read a previous version.
2. **Concurrent readers**: another reader may currently hold an older snapshot (mid-query) — deleting immediately would cause that query to fail partway through.

Consequence: without cleanup, storage grows indefinitely over time even if the "logical" data doesn't change much. `VACUUM` is the tool that cleans up files that have **been `remove`d AND are older than the retention threshold** (no longer belonging to any version within the allowed time-travel window).

### 8.2. Syntax

```sql
VACUUM db.tbl;                          -- default retention: 7 days (168 hours)
VACUUM db.tbl RETAIN 168 HOURS;         -- explicit
VACUUM db.tbl RETAIN 24 HOURS DRY RUN;  -- ONLY lists files that would be deleted, doesn't delete
```

- **Default retention: 7 days (168 hours)** — this number is chosen to be larger than the maximum time a running query or a concurrent transaction could plausibly still be active.
- `VACUUM` **refuses** retention values below 168 hours unless the safety check is disabled:
```sql
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM db.tbl RETAIN 1 HOURS;   -- only use when you TRULY understand the risk (e.g. a test environment)
```
- **Always run `DRY RUN` first** in any important environment to preview the list of files that would be deleted.

### 8.3. Retention trade-offs

| Short retention | Long retention |
|---|---|
| Reclaims storage costs faster | Enables time travel further into the past |
| Risk: a running query/transaction can fail if a file is deleted while it's still in use | Costs more in "leftover" storage |
| Suited to tables written very frequently (lots of garbage) | Suited to tables requiring long-term audit/compliance |

`VACUUM` **does not** delete anything in `_delta_log` (JSON logs) — that's the job of `delta.logRetentionDuration` (30-day default); Delta cleans up old logs through its own separate mechanism, independent of data-file VACUUM.

---

## 9. Change Data Feed (CDF)

**Goals:** enable Change Data Feed on a Delta table; read **only the changed portion** (insert/update/delete) between two versions instead of re-reading the entire table; understand the special columns: `_change_type`, `_commit_version`, `_commit_timestamp`.

### 9.1. The problem CDF solves

Without CDF, to know "what has changed since the last sync" between two systems (e.g. pushing data from a silver table into another datamart), you would have to:
- Compare the entire old and new snapshots (expensive), or
- Build your own custom change-tracking mechanism.

**Change Data Feed** lets Delta Lake automatically record **record-level changes** (inserts, updates, deletes) every time `UPDATE`/`DELETE`/`MERGE`/a streaming write happens, enabling efficient incremental reads — the foundation for CDC (Change Data Capture) pipelines.

### 9.2. Enabling CDF

```sql
ALTER TABLE db.tbl SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```
Or at table creation:
```sql
CREATE TABLE db.tbl (...) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);
```
> CDF only records changes **from the moment it's enabled onward** — versions prior to enabling have no change data.

### 9.3. Reading the change feed

```sql
SELECT * FROM table_changes('db.tbl', 2, 5);              -- from version 2 to version 5
SELECT * FROM table_changes('db.tbl', '2024-01-01', '2024-01-31');  -- by time range
```
Or via DataFrameReader:
```python
spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 2) \
    .option("endingVersion", 5) \
    .table("db.tbl")
```

The result includes **all original columns** plus 3 metadata columns:
- `_change_type`: `insert`, `update_preimage` (the value *before* the update), `update_postimage` (the value *after* the update), `delete`.
- `_commit_version`: the version that produced this change.
- `_commit_timestamp`: the commit time.

A single `UPDATE` produces **two rows** in the change feed for one modified record: one `update_preimage` row + one `update_postimage` row — allowing the exact before/after values to be known.

---

## 10. Structured Streaming with Delta Lake

**Goals:** use a Delta table as both a **streaming source** (`readStream`) and a **streaming sink** (`writeStream`); understand the role of **checkpoint location** and the different `trigger` types (`availableNow`, `processingTime`, `once`); use `foreachBatch` to run `MERGE` (upsert) inside a streaming pipeline — the very common **stream → Delta upsert** pattern.

### 10.1. Delta Lake as "unified batch & streaming"

Since every change to a Delta table is a sequence of **ordered commits** in `_delta_log`, Spark Structured Streaming can treat a Delta table as an **infinite stream of appends** — on each read, the engine only needs to know "which commit has already been processed" (stored in the checkpoint) and then read any newer commits. This is what makes Delta a natural bridge between batch and streaming: **the same table** can simultaneously receive writes from a streaming job and be read normally via SQL by a separate batch job.

### 10.2. Reading a Delta table as a stream

```python
stream_df = spark.readStream.format("delta").table("db.source_tbl")
```
By default, the streaming source **only reads `append` operations** that have occurred since the query started (or since the checkpoint). If the source has `UPDATE`/`DELETE`/`overwrite` operations, this **raises an error** by default (since that isn't purely "adding new data") — unless `option("ignoreChanges", "true")` or `option("ignoreDeletes", "true")` is enabled, or CDF is used (`option("readChangeFeed", "true")`, see Section 9) to correctly receive both updates and deletes.

### 10.3. Writing to a Delta table as a stream

```python
(stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", "s3a://.../_checkpoints/my_stream")
    .trigger(availableNow=True)     # process all currently available data, then stop (like a scheduled batch job)
    .toTable("db.sink_tbl"))
```

- **`checkpointLocation`**: where Spark stores progress (how far processing has gotten) — **mandatory**, and **unique per query** (never shared between two different queries).
- **Trigger types**:
  - `trigger(processingTime="10 seconds")` — runs continuously on a fixed cycle, suited to "true" 24/7 streaming.
  - `trigger(availableNow=True)` — processes all currently available data then stops automatically; used for schedule-driven pipelines (like an Airflow trigger every hour) while still benefiting from streaming's checkpoint/exactly-once semantics.
  - `trigger(once=True)` — similar to `availableNow` but processes everything in a single batch (not split up); deprecated, `availableNow` is recommended instead.

### 10.4. `foreachBatch` — running MERGE in streaming

To perform an **upsert** (not just append) in a streaming sink, use `foreachBatch` to apply arbitrary logic (including `MERGE`) to each micro-batch:

```python
def upsert_batch(batch_df, batch_id):
    (DeltaTable.forName(spark, "db.sink_tbl").alias("t")
        .merge(batch_df.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

(stream_df.writeStream
    .foreachBatch(upsert_batch)
    .option("checkpointLocation", "...")
    .trigger(availableNow=True)
    .start())
```
This is the standard pattern for **CDC ingestion**: continuously read changes (via CDF or another source), then `MERGE` them into the destination table instead of just appending.

---

## 11. Concurrency Control & Transaction Conflicts

**Goals:** understand **optimistic concurrency control (OCC)** in Delta Lake — why traditional locking isn't used; recognize the exceptions raised when two transactions conflict: `ConcurrentAppendException`, `ConcurrentDeleteReadException`, etc.; understand the 2 isolation levels: `Serializable` and `WriteSerializable`.

### 11.1. Why Optimistic Concurrency Control?

Object storage (S3/MinIO) doesn't support multi-process locking the way a traditional database does. Delta Lake handles concurrent writes using **optimistic concurrency control**:

1. Each transaction reads the table's **current version** when it starts (snapshot isolation on read).
2. The transaction does its work (computation, writing new Parquet files) **entirely independently**, without locking the table.
3. On commit, the transaction attempts to write the file `_delta_log/{N+1}.json` — this relies on storage's atomic **"put-if-absent"** capability (only one writer can successfully create a file with the name `N+1`; other writers attempting the same name fail).
4. If it loses the race for version `N+1`, the losing transaction performs a **conflict check**: comparing what it depended on (files it read, WHERE conditions...) against what the winning transaction just changed.
   - If there is **no real logical conflict** (e.g. two transactions appending to two different partitions) → it automatically **rebases** onto the new version and **retries** the commit as version `N+2`.
   - If there **is** a real conflict (both touch the same file/same row of data) → the losing transaction is aborted and an exception is thrown up to the application layer — the application must decide whether to retry.

### 11.2. Common conflict exceptions

| Exception | Meaning |
|---|---|
| `ConcurrentAppendException` | Two transactions both add files to the same data region (e.g. two `MERGE`/`UPDATE` operations touching the same partition) where one of them needs to read a stable set of files |
| `ConcurrentDeleteReadException` | A transaction is reading a file that another transaction has already `remove`d |
| `ConcurrentDeleteDeleteException` | Two transactions both try to remove the same file |
| `MetadataChangedException` | Schema/table properties were changed (e.g. `ALTER TABLE`) while another transaction was running |
| `ProtocolChangedException` | The table's reader/writer protocol version was upgraded midway |
| `ConcurrentTransactionException` | Two transactions share the same `appId`/`txnVersion` (commonly seen when a streaming source is accidentally run twice) |

When these exceptions occur, the usual application-layer response is to **retry the entire transaction** (re-read the latest data, then try writing again) — Delta deliberately does not retry automatically on your behalf, because the transaction's computation logic may depend on the data it read (a silent retry could produce an incorrect result).

### 11.3. Isolation Levels

- **`Serializable`** (the default for most operations: UPDATE/DELETE/MERGE) — the strictest isolation level, guaranteeing a result equivalent to running transactions sequentially.
- **`WriteSerializable`** (the default for **blind appends**, i.e. writes that only add data without reading current state) — slightly relaxed to allow more parallel writes, while remaining safe for pure-append scenarios.

Configured via table property: `ALTER TABLE db.tbl SET TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')`.

---

## 12. Cloning Tables (SHALLOW/DEEP CLONE) & Synthesis

**Goals:** distinguish between `SHALLOW CLONE` and `DEEP CLONE`, and know when to use each; tie together the full body of Delta Lake knowledge; have a best-practices checklist to apply to a real project.

### 12.1. `SHALLOW CLONE` vs `DEEP CLONE`

```sql
CREATE TABLE db.tbl_shallow SHALLOW CLONE db.tbl_source;
CREATE TABLE db.tbl_deep    DEEP CLONE    db.tbl_source;
```

| | **SHALLOW CLONE** | **DEEP CLONE** |
|---|---|---|
| Copies physical data (Parquet files)? | **No** — only copies metadata (`_delta_log`), pointing to the original files | **Yes** — copies all data files to the new location |
| Creation speed | Very fast (only a few JSON files written) | Slower, proportional to data size |
| Independent from the source table? | **Not fully** — if the source table is `VACUUM`ed and old files are deleted, the clone may lose the ability to read the corresponding data | **Yes** — fully independent, safe even if the source table is deleted/VACUUMed |
| Use case | Quick experimentation (trying a schema change, trying `OPTIMIZE`, trying a new pipeline) without extra storage cost | Backups, creating production snapshots for dev/staging testing, migrating to a different catalog/storage |

Both clone types create a **brand-new, write-independent** Delta table: writing/updating/deleting on the clone **does not affect** the source table (and vice versa) — because each table has its own `_delta_log`, and any changes made after cloning are only recorded in the respective log.

A clone can be taken at a specific version: `CREATE TABLE db.snap DEEP CLONE db.tbl VERSION AS OF 5;` — combining Time Travel (Section 5) with Clone to produce a production snapshot at a specific point in time.

### 12.2. How the concepts fit together

A realistic pipeline typically combines the techniques above in this order:

1. **(Sections 2, 6)** Create a raw ("bronze") table with data constraints (e.g. `qty > 0`) and Change Data Feed enabled.
2. **(Section 3)** Load initial data via `append`, partitioned by a suitable low-cardinality column such as a date.
3. **(Section 4)** Use `MERGE INTO` to upsert incoming correction/update batches (some existing rows updated, some brand-new rows inserted).
4. **(Section 9)** Read the **Change Data Feed** to capture exactly what changed in that upsert.
5. **(Section 10)** Use a `foreachBatch` streaming job to propagate those changes into a downstream aggregate/summary table.
6. **(Sections 7, 8)** Periodically run `OPTIMIZE ... ZORDER BY` on the bronze table, then `VACUUM` (with `DRY RUN` first) to reclaim space from obsolete files.
7. **(Section 5)** Use `DESCRIBE HISTORY` + `VERSION AS OF` to audit every step above.
8. **(Section 12.1)** Take a `DEEP CLONE` of the bronze table as a backup snapshot before further experimentation.

**Operational checklist for a production table:**
- **`OPTIMIZE`** frequency should scale inversely with write frequency (more frequent, smaller writes → run OPTIMIZE more regularly, e.g. daily); add `ZORDER BY` on high-cardinality columns that are frequently filtered but not practical as partition columns (Section 7).
- **`VACUUM`** should keep the default 7-day retention unless there's a clear reason to lower it (test environment, severe storage-cost pressure); always `DRY RUN` first in production (Section 8).
- **CDF** should be enabled if any downstream consumer needs incremental change reads (CDC to another system, analytics sync...); not needed if the table is only ever read via full scans.
- **Partitioning vs. Z-ordering** should be chosen based on cardinality and how often a column appears in filters, following the principles from Sections 3 and 7.
