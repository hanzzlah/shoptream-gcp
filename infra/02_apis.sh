#!/bin/bash
gcloud services enable \
  bigquery.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com \
  cloudkms.googleapis.com \
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  dataproc.googleapis.com \
  workflows.googleapis.com \
  cloudscheduler.googleapis.com \
  eventarc.googleapis.com \
  cloudfunctions.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  logging.googleapis.com \
  bigtable.googleapis.com \
  bigtableadmin.googleapis.com