#!/bin/bash
gcloud functions delete shopstream-csv-ingest --gen2 --region="$REGION" --quiet
gcloud scheduler jobs delete shopstream-daily-batch --location="$REGION" --quiet
gcloud scheduler jobs delete shopstream-temp-cleanup --location="$REGION" --quiet
gcloud workflows delete shopstream-daily-batch --location="$REGION" --quiet
gcloud workflows delete shopstream-temp-cleanup --location="$REGION" --quiet
gcloud pubsub subscriptions delete "$PUBSUB_SUB" --quiet
gcloud pubsub topics delete "$PUBSUB_TOPIC" --quiet
gcloud bigtable instances delete "$BIGTABLE_INSTANCE" --quiet
gcloud storage rm --recursive "gs://${BUCKET}/temp/**"
gcloud storage rm --recursive "gs://${BUCKET}/dataflow/**"
gcloud projects delete "$PROJECT_ID" --quiet