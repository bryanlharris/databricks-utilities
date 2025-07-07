#!/usr/bin/env bash

JOB_ID=242973079672543

PROFILE=${DATABRICKS_PROFILE:+-p "$DATABRICKS_PROFILE"}

databricks $PROFILE jobs list-runs --job-id "$JOB_ID" --output json | jq -r '.[].run_id' | while read -r RUN_ID; do
    databricks $PROFILE jobs delete-run "$RUN_ID"
done
