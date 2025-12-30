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

[![High level diagram](https://github.com/vishweshhampali/odi-cricket-analytics-aws/blob/main/AWS_pipeline.jpg "High level diagram")]
