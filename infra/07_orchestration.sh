#!/bin/bash
gcloud workflows deploy shopstream-daily-batch --source=workflows/daily_batch.yaml --location="$REGION" --service-account="$SA_EMAIL"
gcloud workflows deploy shopstream-temp-cleanup --source=workflows/cleanup_temp.yaml --location="$REGION" --service-account="$SA_EMAIL"

gcloud scheduler jobs create http shopstream-daily-batch \
  --location="$REGION" \
  --schedule="0 2 * * *" \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/workflows/shopstream-daily-batch/executions" \
  --http-method=POST \
  --oauth-service-account-email="$SA_EMAIL" \
  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
  --message-body="{\"argument\":\"{\\\"project_id\\\":\\\"${PROJECT_ID}\\\",\\\"region\\\":\\\"${REGION}\\\",\\\"bucket\\\":\\\"${BUCKET}\\\",\\\"service_account\\\":\\\"${SA_EMAIL}\\\",\\\"jdbc_url\\\":\\\"${PG_JDBC_URL}\\\",\\\"jdbc_user\\\":\\\"${PG_USER}\\\",\\\"jdbc_password\\\":\\\"${PG_PASSWORD}\\\"}\"}"

gcloud scheduler jobs create http shopstream-temp-cleanup \
  --location="$REGION" \
  --schedule="30 2 * * *" \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/workflows/shopstream-temp-cleanup/executions" \
  --http-method=POST \
  --oauth-service-account-email="$SA_EMAIL" \
  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
  --message-body="{\"argument\":\"{\\\"bucket\\\":\\\"${BUCKET}\\\"}\"}"