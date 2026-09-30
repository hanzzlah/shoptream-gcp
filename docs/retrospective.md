# ShopStream: Project Retrospective

## 1. What Went Well?
* **Unified Data Ingestion:** Successfully integrated three disparate data paradigms (batch/historical, real-time streaming, and ad-hoc event-driven uploads) into a single BigQuery Data Warehouse.
* **Serverless Architecture:** Leveraged GCP's fully managed services (Dataproc Serverless, Cloud Functions Gen2, Dataflow, Cloud Workflows) to ensure the platform scales automatically and only incurs charges for exact compute time used.
* **Data Modeling:** Implemented a clean Medallion Architecture (Staging -> Curated -> Marts) to create a single source of truth, making downstream reporting easy and efficient.

## 2. What Was the Most Challenging Part?
* **IAM and Permissions:** Getting Eventarc to successfully trigger Cloud Functions Gen2 required precise role bindings, specifically discovering that the Cloud Storage Service Agent needed the `roles/pubsub.publisher` role to dispatch events.
* **Dependency Isolation:** Learning to manage dependencies correctly by using separate `requirements.txt` files for Dataflow (Beam) and Cloud Functions. This was crucial to avoid bloated containers and slow cold starts in the event-driven pipeline.
* **External Database Connectivity:** Connecting Dataproc Serverless to an external Supabase PostgreSQL database required careful handling of the PostgreSQL JDBC jar file, environment variables, and URL-encoding complex passwords.
* **STTTTTTRRRRRRRRRRREEEEEEEEEAAAAAAAAAAAAAAAMMMMMMMMMIIIIIIIIINNNNNGGGGGGG**

## 3. How Were Costs Managed and Prevented?
* **Strict Teardown Automation:** Created a comprehensive `99_cleanup.sh` script to instantly wipe all billable compute resources (Pub/Sub topics/subs, Cloud Functions, Schedulers) at the end of the development session.
* **Manual Streaming Intervention:** Always-on streaming (Dataflow) is expensive. We intentionally and immediately cancelled the streaming job via CLI right after verifying the data landed in BigQuery to prevent draining trial credits.
* **Storage Hygiene:** Built and deployed a Cloud Workflow triggered by Cloud Scheduler to automatically crawl and clean up GCS `temp/` and `dataflow/` staging buckets every night.
* **Strategic Omissions:** Opted to skip the Bigtable deployment for the initial proof-of-concept to stay strictly within the free-tier/low-cost budget constraints.

## 4. Next Steps & Future Improvements
* **Infrastructure as Code (IaC):** Replace the standard bash scripts (`infra/*.sh`) with Terraform for more robust state management, drift detection, and easier replication of the infrastructure.
* **Data Transformations (dbt):** Replace the raw BigQuery SQL statements used for the Curated and Mart layers with `dbt` to introduce data testing, automated documentation, and CI/CD version control for analytics.
* **CI/CD Pipeline:** Implement GitHub Actions to automatically deploy updates to the Cloud Function, Dataproc PySpark scripts, and Workflows upon pushing to the `main` branch.
* **BI Integration:** Connect a visualization tool like Data Studio to the `shopstream_marts` dataset to build a live, interactive dashboard for stakeholders.
