# 📚 Open Library Serverless ETL Pipeline

A serverless, event-driven ETL pipeline built on AWS that ingests book and cover metadata from the [Open Library API](https://openlibrary.org/developers/api), transforms it using Polars, and loads structured outputs into S3 — automatically cataloged by AWS Glue and validated via AWS Athena.

---

## 📖 Introduction

This project is a serverless ETL pipeline built on AWS. Coming from a background of running jobs on servers, I wanted to challenge myself with a fully serverless, event-driven architecture, and use it as an opportunity to build something end-to-end as part of my data engineering journey.
The pipeline pulls book and cover metadata from the Open Library API on a 24-hour schedule, transforms the raw responses into structured data using Polars, and stores the results in S3. Each Lambda function triggers the next via S3 events. At the end of the pipeline, AWS Glue catalogs the Parquet outputs and Athena validates that the data was correctly loaded and queryable.

I chose the Open Library API because it's free, well-documented, and while simple, it produces messy enough real-world data that forces you to think carefully about what the transformation should look like.

---

## 🛠️ Technologies Used

| Category | Tool / Service |
|---|---|
| Cloud Platform | AWS |
| Compute | AWS Lambda (Python 3.11) |
| Scheduling / Orchestration | AWS EventBridge, S3 Event Triggers |
| Storage | AWS S3 |
| Data Cataloging | AWS Glue Crawler |
| Query & Validation | AWS Athena |
| Data Processing | [Polars](https://pola.rs/) |
| Data Source | [Open Library API](https://openlibrary.org/developers/api) |
| Dependency Management | AWS Lambda Layers |
| Language | Python 3.11 |

---

## ✨ Features

- **Fully serverless** — no EC2, no containers, no infrastructure to manage
- **Sequential, event-driven execution** — each Lambda is triggered by an S3 event from the previous step, forming an automatic chain
- **Two parallel data tracks** — book metadata and cover metadata are extracted, transformed, and stored independently
- **Raw data preservation** — raw API responses land in `to_process/` and are moved to `processed/` after transformation, enabling reprocessing without re-fetching
- **Multi-format output** — each transform phase produces JSON, CSV, and Parquet outputs
- **Shared utility layers** — common S3 and API logic is packaged as a reusable Lambda Layer, keeping handler code clean
- **Glue Crawler cataloging** — Parquet outputs are automatically cataloged into the AWS Glue Data Catalog
- **Athena validation** — post-crawl queries confirm the data was correctly cataloged and is queryable

---

## ⚙️ Architecture & Process

```
EventBridge (Scheduled Rule)
        │
        ▼
 Lambda: Ingest
  - Calls Open Library API
  - Stores raw JSON response
  - Writes to S3 (raw bucket)
        │
        ▼
 Lambda: Transform
  - Reads raw JSON from S3
  - Cleans + reshapes data with Polars
  - Writes structured output to S3 (processed bucket)
        │
        ▼
   S3: Processed Bucket
  (structured, query-ready data)
```

**Step-by-step flow:**

1. EventBridge triggers the **Ingest Lambda** on a schedule
2. Ingest calls the Open Library API and writes the raw JSON response to `s3://raw-bucket/`
3. The **Transform Lambda** reads the raw file, unpacks nested fields, standardizes types, and drops nulls/irrelevant columns using Polars
4. The cleaned dataset is written to `s3://processed-bucket/` in structured format

---

## 💡 What I Learned

- Gained hands-on experience with core AWS services such as Lambda, S3, Glue, and Athena, and how they connect to form a complete serverless pipeline.
- Debugged Lambda execution errors using CloudWatch Logs, learning to trace failures across a multi-function pipeline.
- Learned what Lambda Layers are and how to use them (both for third-party dependencies like Polars and for shared utility code), including the constraints around runtime compatibility and packaging structure.
- Developed a clearer mental model of event-driven architecture in AWS (how triggers, payloads, and function boundaries interact).
- Learned that each Lambda function requires its own IAM role and permissions.
- Learned why Parquet is the preferred format for AWS Glue. Its columnar structure and schema metadata make crawling and cataloging significantly more reliable than CSV or JSON.
- Got hands-on experience with Polars for the first time.

---

## 🔧 How It Can Be Improved

- **Add an orchestration layer** — use AWS Step Functions or Airflow to explicitly chain ingest → transform with error handling and retries between steps
- **Schema validation** — validate raw API responses against an expected schema before writing to S3, catching upstream API changes early
- **Incremental loading** — currently re-fetches all data on each run; could track last-fetched state to only pull new records
- **Alerting** — add CloudWatch alarms or SNS notifications on Lambda failures so the pipeline doesn't fail silently
- **Infrastructure as Code** — define all AWS resources (Lambda, S3, EventBridge, IAM roles) in Terraform or AWS CDK for reproducible deploys
- **Unit tests** — add pytest coverage for the transform logic, especially the Polars cleaning steps
- **Continue Learning** — continue to learn more about serverless functions and the AWS ecosystem

---

## Author

**Juan Quezada**
[GitHub](https://github.com/yourusername)