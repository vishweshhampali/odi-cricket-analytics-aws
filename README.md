# Building an End-to-End ODI Cricket Analytics Pipeline on AWS (Lambda, Glue, Redshift, QuickSight)

I’ve always enjoyed sports analytics, and cricket is the perfect domain for it, there’s rich ball-by-ball data, and it naturally leads to questions you can answer with good modelling and reporting.

Instead of doing a one-off notebook analysis, I wanted to build something closer to how real analytics platforms work: ingest raw data reliably, transform it at scale, model it properly in a warehouse, and visualise it in BI.

So I built an end-to-end ODI cricket analytics pipeline on AWS that takes raw JSON scorecards and turns them into a Redshift star schema, with batsman-focused marts powering an Amazon QuickSight dashboard.

### Tech Stack

- AWS Lambda – ingestion + deduplication
- Amazon S3 – raw + curated data lake
- AWS Glue (PySpark) – parsing + flattening + Parquet outputs
- Amazon Redshift – staging + dimensional model + incremental upserts
- Amazon QuickSight – dashboard on top of marts/views

*Code + SQL scripts: [https://github.com/vishweshhampali/odi-cricket-analytics-aws]*

## Architecture Overview

![High level diagram](https://github.com/vishweshhampali/odi-cricket-analytics-aws/blob/main/AWS_pipeline.png)

At a high level, the pipeline does this:
1. Ingest ODI JSON scorecards once and store them in S3 (raw)
2. Transform nested JSON into analytics-friendly tables using Glue (PySpark)
3. Load curated datasets into Redshift staging and then upsert into a star schema
4. Build mart views/materialized views focused on batsman performance
5. Visualise those marts in QuickSight


------------

### 1. Ingestion: AWS Lambda + S3 (incremental and idempotent)

The source data comes as a ZIP of ODI JSON scorecards (Cricsheet-style format).

The ingestion Lambda does the following:
- Downloads the ODI ZIP from a configured URL
- Extracts JSON files in-memory (using Lambda’s /tmp)
- Calculates a SHA-256 checksum for each match file
- Checks S3 for duplicates:
	- If a file with the same checksum already exists → skip
	- If it’s new/changed → upload to S3
- Writes a manifest file for each run (stored in _manifest/), containing:
	- match file name
	- checksum
	- ingestion timestamp/date

**Reasons I did it this way**
- It keeps the pipeline incremental (only new/changed matches are processed).
- It’s idempotent (running it again won’t duplicate data).
- The manifest makes runs traceable (great for debugging and audits)

**Raw storage**
- Example bucket: my-raw-odi-bucket.
- Path pattern: s3://.../raw/odi/ and s3://.../raw/odi/_manifest/


### 2. Transformation: AWS Glue + PySpark (flatten nested JSON to tables)

Raw scorecards are great for humans, but they’re deeply nested (innings → overs → deliveries). That structure is not ideal for SQL analytics.

So I built a Glue PySpark job that:
- Reads ingested JSON files from S3
- Uses the manifest to process only the latest ingestion batch (or a selected date)
- Flattens the data into three core tables.

**Output tables (curated layer)**
1. match_table – one row per match (metadata)
2. player_table – one row per player per match (squads / roles)
3. delivery_table – one row per ball (innings, over, batter, bowler, runs, extras, wickets, etc.)

The Glue job writes these as Parquet back to S3 (curated zone), partitioned by ingest date.

**Why Parquet + partitioning**
- Parquet is compressed, columnar, and fast for analytics.
- Partitioning by ingest_date makes incremental loads efficient.
- I also keep a convenient latest/ folder to simplify early Redshift COPY patterns

*Glue script reference: json2csv_spark.py (in above repo)*

### 3. Warehouse: Redshift staging + star schema (dimensions & facts)

Once the curated Parquet datasets are ready, I load them into Redshift in three layers:
- stg: staging tables (mirror Glue outputs)
- dw: dimensional model (facts + dimensions)
- mart: BI-friendly views/materialized views.

**Staging layer (stg)**
1. stg.match_table
2. stg.player_table
3. stg.delivery_table

These are loaded using Redshift COPY from the curated Parquet paths.

Staging gives me a clean checkpoint: I can validate counts, nulls, duplicates, and rerun loads safely.

### 4. Dimensional modelling: building reusable dimensions

From staging, I populate dimension tables using MERGE (upsert) logic.

**Dimensions (dw.dim_*)**
1. dw.dim_player
2. dw.dim_team
3. dw.dim_venue
4. dw.dim_date
5. dw.dim_matc

**Design highlights (in simple terms)**
- Players, teams, venues get surrogate keys for stable joins
- dim_date standardises date filtering (year, month, weekday, etc.)
- dim_match links match metadata to teams, venue, and date

This structure I believe is much easier for analytics than repeatedly joining raw text fields.

### 5. Fact table: one row per ball (dw.fact_delivery)

The central fact table is dw.fact_delivery, with one row per delivery, including the following.

**Keys (for joining)**
- match_key
- batting_team_key, bowling_team_key
- striker_player_key, non_striker_player_key, bowler_player_key
- venue_key, date_key (via match)

**Natural identifiers**
- match_id
- innings, over, ball_in_over

**Measures & flags**
- runs_off_bat, extras, total_runs
- is_boundary_4, is_boundary_6
- is_wicket, dismissal_kind

**Metadata**
- ingest_date
- stg_modified_ts (useful for late corrections)

**Deduplication Rule**

If the same delivery appears again due to source corrections, I keep the latest version using a simple rule like:
*“take the most recent stg_modified_ts per (match_id, innings, over, ball).”*

### 6. Mart layer: batsman-focused materialized views

A ball-level fact table is powerful, but not ideal for BI users. Most dashboard users don’t want to aggregate from “one row per ball” every time.

So I created a mart layer with batsman-focused materialized views. Example views:

**mart.batsman_summary_mv - One row per batsman per match:**

- runs, balls, 4s, 6s
- strike rate
- dismissal info
- opponent, venue, match date

**mart.batsman_career_mv - Career-level aggregates:**

- matches, innings, total runs, total balls
- average, strike rate
- boundaries

**mart.batsman_dismissal_mv - Dismissal breakdown:**

- bowled / caught / LBW / run out etc. counts

These marts make QuickSight fast and easy to use.

*Mart definition reference: DDL_MART_BATSMAN.py (in above repo)*

### 7. Visualisation: QuickSight dashboard

On top of the mart layer, I built a Batsman Performance dashboard in Amazon QuickSight.

The dashboard allows you to select a batsman and explore:

- run and strike-rate trends over time
- performance vs opponents
- best/worst venues
- dismissal patterns

![High level diagram](https://github.com/vishweshhampali/odi-cricket-analytics-aws/blob/main/dashboard_image.png)

Please follow the link for PDF version [click here](https://github.com/vishweshhampali/odi-cricket-analytics-aws/blob/main/Sheet_1_2025-12-29T19_12_46.pdf "click here").

### Challenges and Learnings

**Nested JSON complexity**
Cricket scorecards have multiple levels of nesting. Designing the Spark schema and flattening logic carefully was essential.

**Incremental ingestion (no reprocessing everything)**
Checksum-based deduplication + manifest files made ingestion repeatable, traceable, and easy to debug.

**Dimensional modelling for sports data**
It was a solid real-world exercise in star schema design:
- stable keys
- reusable dimensions
- facts that work for multiple reporting needs

**Designing for BI usability (not just “data exists”)**
The mart layer made the difference between “data in a warehouse” and “data people can actually use”.

### Future scope for improvements

**If I extend the platform, I wouldd add:**

- orchestration (Step Functions or MWAA/Airflow)
- automated data quality tests (e.g., null/duplicate thresholds)
- monitoring/alerts (CloudWatch metrics + notifications)
- dbt-style modelling for the mart layer
- CI/CD for deploying SQL + Glue jobs
