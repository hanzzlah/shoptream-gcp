# ShopStream: End-to-End GCP Data Platform Presentation

---

## Slide 1: Title Slide
* **Title:** ShopStream: Ultra-Low-Cost GCP Data Platform
* **Subtitle:** Batch, Streaming, and Event-Driven Ingestion at Scale
* **Presenters:** Hanzlah & Hasaan
* **Key Takeaway:** Building a robust, serverless data platform on Google Cloud that minimizes costs without sacrificing real-time capabilities.

---

## The Business Challenge
* **The Problem:** 
  * "ShopStream" (a mock e-commerce platform) has fragmented data. 
  * Historical records are stuck in a PostgreSQL database.
  * Real-time user events stream in continuously.
  * Third-party vendors upload daily CSV files inconsistently.
* **The Constraint:** Always-on data infrastructure (like persistent Spark clusters) is too expensive for a startup budget.
* **The Goal:** Unify these three data sources into a single source of truth for analytics using a pay-per-use model.

---

## The Serverless Solution (Architecture Overview)
* **Approach:** A fully serverless Medallion Architecture on Google Cloud Platform.
* **Ingestion Paths:**
  1. **Batch:** For heavy, historical data loads.
  2. **Streaming:** For low-latency real-time events.
  3. **Event-Driven:** For ad-hoc, unpredictable file uploads.
* **Storage Hub:** BigQuery (Data Warehouse) and Bigtable (Low-latency application lookups).

---

## Pipeline 1 - Batch Ingestion
* **Source:** Supabase (PostgreSQL).
* **Compute:** Dataproc Serverless (PySpark).
* **How it works:** 
  * A transient PySpark job spins up, connects to PostgreSQL via JDBC, and extracts historical orders.
  * Cleanses data (removes negative quantities), deduplicates, and writes directly to BigQuery `shopstream_staging`.
* **Why it matters:** Dataproc Serverless ensures we only pay for the exact compute time used during the extraction, zero idle costs.

---

## Pipeline 2 - Real-Time Streaming
* **Source:** Live event streams (simulated via JSONL).
* **Broker:** Cloud Pub/Sub.
* **Compute:** Cloud Dataflow (Apache Beam).
* **How it works:** 
  * Dataflow consumes messages from a Pub/Sub subscription.
  * Validates JSON schema in-flight.
  * *Dual-write:* Streams enriched data into BigQuery (for analytics) AND Bigtable (for low-latency customer lookups).

---

## Pipeline 3 - Event-Driven File Processing
* **Source:** Manual CSV uploads to Cloud Storage (GCS).
* **Trigger:** Eventarc (Object Finalized Event).
* **Compute:** Cloud Functions Gen2 (Python 3.11).
* **How it works:** 
  * When a vendor drops a CSV in the `raw/uploads/` bucket, Eventarc instantly triggers the Cloud Function.
  * The function validates the headers and data types. Valid rows are appended to BigQuery; invalid files throw alerts in Cloud Logging.

---

## Data Modeling (The Medallion Architecture)
* **Bronze (Staging):** Raw data from Batch, Streaming, and Uploads stored exactly as received.
* **Silver (Curated):** The `orders_enriched` table. Data from all three sources is combined, deduplicated, and standardized.
* **Gold (Marts):** Business-ready dimensional models.
  * `dim_customer`: Lifetime value and order counts.
  * `dim_product`: Product catalog dimensions.
  * `agg_daily_sales`: Ready for BI dashboards.

---

## Orchestration & Automation
* **Tools Used:** Cloud Workflows & Cloud Scheduler.
* **Daily Automation:** 
  * Cloud Scheduler triggers a workflow every night at 2:00 AM.
  * The Workflow securely passes database credentials to Dataproc to kick off the batch job.
* **Cost-Hygiene Automation:** A secondary workflow automatically crawls the GCS `temp/` buckets and deletes temporary spark/dataflow staging files to prevent storage bloat.

---

## Security & Governance
* **Principle of Least Privilege:** Custom IAM Service Accounts used for all services. No generic editor roles.
* **Encryption:** Customer-Managed Encryption Keys (CMEK) via Cloud KMS applied to all GCS buckets and BigQuery datasets.
* **Cost Controls:** Strict teardown bash scripts provided to instantly wipe all billable resources at the end of the day.

---

## Retrospective & Next Steps
* **What Went Well:** Successfully unified three disparate data patterns into one warehouse using 100% serverless infrastructure.
* **Challenges Overcome:** Managing correct IAM permissions for Eventarc and Cloud Storage to trigger Gen2 Functions.
* **Next Steps:** 
  * Connect a BI Tool (Looker Studio) to the `shopstream_marts` dataset.
  * Implement dbt (Data Build Tool) to replace raw BigQuery SQL transformations for better version control.
