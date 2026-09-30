# ShopStream: Architecture & Infrastructure Documentation

This document outlines the infrastructure components, data flow, and data modeling strategies used in the ShopStream end-to-end data platform on Google Cloud Platform (GCP).

## 1. High-Level Architecture

The platform is designed to handle three distinct types of data ingestion, unifying them into a single analytical data warehouse in BigQuery.

*   **Source Systems:** Supabase (PostgreSQL), Pub/Sub (Streaming Events), Cloud Storage (CSV Uploads).
*   **Processing Engine:** Dataproc Serverless (Batch), Dataflow (Streaming), Cloud Functions Gen2 (Event-Driven).
*   **Storage & Analytics:** BigQuery (Data Warehouse), Cloud Storage (Data Lake/Staging), Bigtable (Low-latency lookups).
*   **Orchestration:** Cloud Workflows and Cloud Scheduler.

## 2. Infrastructure Components

### A. Batch Pipeline (Historical Data)
*   **Compute:** Dataproc Serverless (PySpark)
*   **Process:** A daily scheduled PySpark job connects to the Supabase PostgreSQL database via JDBC. It extracts historical orders, performs basic cleansing (filtering negative quantities/prices), deduplicates based on the latest `updated_at` timestamp, and writes the output directly to BigQuery using the BigQuery Storage Write API.

### B. Streaming Pipeline (Real-Time Events)
*   **Message Broker:** Cloud Pub/Sub
*   **Compute:** Cloud Dataflow (Apache Beam)
*   **Process:** An always-on Apache Beam pipeline reads JSON messages from a Pub/Sub subscription. It validates the schema, drops malformed records, adds ingestion metadata, and streams the validated records simultaneously into **BigQuery** (for historical analytics) and **Bigtable** (for real-time, low-latency customer order lookups).

### C. Event-Driven Pipeline (Manual Uploads)
*   **Trigger:** Eventarc (GCS Object Finalized)
*   **Compute:** Cloud Functions Gen2 (Python 3.11)
*   **Process:** When a CSV file is uploaded to the `raw/uploads/` bucket prefix, Eventarc triggers a Cloud Function. The function parses the CSV, validates data types and required columns, and uses the BigQuery Client Library to insert valid rows into the staging tables.

### D. Security & Orchestration
*   **Security:** Customer-Managed Encryption Keys (CMEK) via Cloud KMS are used to encrypt Cloud Storage buckets and BigQuery datasets. IAM Service Accounts enforce the principle of least privilege.
*   **Orchestration:** Cloud Workflows manage the step-by-step execution of the Dataproc batch jobs and temporary file cleanup, triggered daily by Cloud Scheduler.

## 3. Data Modeling (Medallion Architecture)

The data warehouse in BigQuery follows a multi-layered approach to transform raw data into business-ready insights.

### Staging Layer (`shopstream_staging`)
Raw data is ingested here exactly as it arrives from the source, with added metadata (`source`, `ingested_at`).
*   `raw_orders_batch`
*   `raw_orders_streaming`
*   `raw_orders_uploads`

### Curated Layer (`shopstream_curated`)
Data from all staging tables is unified, deduplicated, and cleansed.
*   `orders_enriched`: The master table combining batch, streaming, and uploaded orders, partitioned/filtered for duplicates.
*   `daily_sales_by_region`: Aggregated daily metrics.
*   `customer_lifetime_value`: Customer-level aggregations.
*   `top_products_by_revenue`: Product performance metrics.

### Mart Layer (`shopstream_marts`)
Highly aggregated and dimensionally modeled tables (Star Schema) built for BI tools and downstream analysts.
*   `dim_customer`
*   `dim_product`
*   `fact_orders`
*   `agg_daily_sales`

## 4. Cross-Source Verification SQL

To ensure all infrastructure pipelines successfully landed data into the staging layer, the following cross-source validation query is used:

```sql
SELECT 'batch' AS source, COUNT(*) AS rows_loaded 
FROM `your_project_id.shopstream_staging.raw_orders_batch`
UNION ALL
SELECT 'streaming', COUNT(*) 
FROM `your_project_id.shopstream_staging.raw_orders_streaming`
UNION ALL
SELECT 'uploads', COUNT(*) 
FROM `your_project_id.shopstream_staging.raw_orders_uploads`;
```