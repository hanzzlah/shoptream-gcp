#!/bin/bash
# Ensure your variables are loaded before running: source .env

gcloud projects create "$PROJECT_ID" --name="ShopStream Junior DE Lab"
gcloud config set project "$PROJECT_ID"
gcloud billing projects link "$PROJECT_ID" --billing-account="BILLING_ACCOUNT_ID"
gcloud config set compute/region "$REGION"