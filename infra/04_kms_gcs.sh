#!/bin/bash
gcloud kms keyrings create "$KMS_KEYRING" --location="$REGION"
gcloud kms keys create "$KMS_KEY" --location="$REGION" --keyring="$KMS_KEYRING" --purpose=encryption

export KMS_KEY_FULL="projects/${PROJECT_ID}/locations/${REGION}/keyRings/${KMS_KEYRING}/cryptoKeys/${KMS_KEY}"
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"

STORAGE_AGENT="$(gcloud storage service-agent --project="$PROJECT_ID")"
gcloud kms keys add-iam-policy-binding "$KMS_KEY_FULL" \
  --member="serviceAccount:${STORAGE_AGENT}" \
  --role="roles/cloudkms.cryptoKeyEncrypterDecrypter"

BQ_AGENT="service-${PROJECT_NUMBER}@gcp-sa-bigquery.iam.gserviceaccount.com"
gcloud kms keys add-iam-policy-binding "$KMS_KEY_FULL" \
  --member="serviceAccount:${BQ_AGENT}" \
  --role="roles/cloudkms.cryptoKeyEncrypterDecrypter"

gcloud storage buckets create "gs://${BUCKET}" \
  --location="$REGION" \
  --default-encryption-key="$KMS_KEY_FULL" \
  --uniform-bucket-level-access

gcloud storage folders create "gs://${BUCKET}/raw/uploads/"
gcloud storage folders create "gs://${BUCKET}/raw/rejected/"
gcloud storage folders create "gs://${BUCKET}/spark/"
gcloud storage folders create "gs://${BUCKET}/temp/"
gcloud storage folders create "gs://${BUCKET}/dataflow/"

bq --location="$REGION" mk --dataset --default_kms_key="$KMS_KEY_FULL" "${PROJECT_ID}:shopstream_staging"
bq --location="$REGION" mk --dataset --default_kms_key="$KMS_KEY_FULL" "${PROJECT_ID}:shopstream_curated"
bq --location="$REGION" mk --dataset --default_kms_key="$KMS_KEY_FULL" "${PROJECT_ID}:shopstream_marts"