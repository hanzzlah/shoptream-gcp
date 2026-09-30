#!/bin/bash
gcloud pubsub topics create "$PUBSUB_TOPIC"
gcloud pubsub subscriptions create "$PUBSUB_SUB" \
  --topic="$PUBSUB_TOPIC" \
  --ack-deadline=30