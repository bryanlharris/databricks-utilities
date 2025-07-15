#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <bucket-name>" >&2
    exit 1
fi

BUCKET="$1"

# List all object versions and delete markers and remove them permanently.
# Run up to 10 deletions in parallel so the machine isn't overwhelmed.
MAX_JOBS=10
current_jobs=0

aws s3api list-object-versions --bucket "$BUCKET" --output json \
    | jq -r '.Versions[]?, .DeleteMarkers[]? | [.Key, .VersionId] | @tsv' \
    | while IFS=$'\t' read -r KEY VERSION_ID; do
        aws s3api delete-object --bucket "$BUCKET" --key "$KEY" --version-id "$VERSION_ID" &
        ((current_jobs++))
        if (( current_jobs >= MAX_JOBS )); then
            wait -n
            ((current_jobs--))
        fi
    done
wait
