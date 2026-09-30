#!/bin/bash
# This project skips this part but here is how you would create a Bigtable instance and table if you wanted to use it in your project.
gcloud bigtable instances create "$BIGTABLE_INSTANCE" \
  --display-name="ShopStream Lab Bigtable" \
  --cluster-config="id=shopstream-cluster,zone=${ZONE},nodes=1" \
  --cluster-storage-type=SSD

gcloud bigtable tables create "$BIGTABLE_TABLE" --instance="$BIGTABLE_INSTANCE"
gcloud bigtable tables create "$BIGTABLE_TABLE" --instance="$BIGTABLE_INSTANCE" --column-families=order