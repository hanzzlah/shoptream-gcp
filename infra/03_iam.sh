#!/bin/bash
gcloud iam service-accounts create "$SA_NAME" \
  --display-name="ShopStream Data Engineer Service Account"

for ROLE in \
  roles/storage.objectAdmin \
  roles/bigquery.dataEditor \
  roles/bigquery.jobUser \
  roles/pubsub.editor \
  roles/dataflow.worker \
  roles/dataproc.worker \
  roles/workflows.invoker
do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$ROLE"
done

# Grant Cloud Storage service agent Pub/Sub Publisher role for Eventarc
STORAGE_AGENT="$(gcloud storage service-agent --project="$PROJECT_ID")"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${STORAGE_AGENT}" \
  --role="roles/pubsub.publisher"