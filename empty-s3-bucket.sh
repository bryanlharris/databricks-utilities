#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <aws-profile> <bucket-name>" >&2
    exit 1
fi

PROFILE="$1"
BUCKET="$2"
AWS_CLI=(aws --profile "$PROFILE")

# List all object versions and delete markers and remove them permanently.
# Run up to 10 deletions in parallel so the machine isn't overwhelmed.
MAX_JOBS=10
current_jobs=0

"${AWS_CLI[@]}" s3api list-object-versions --bucket "$BUCKET" --output json \
    | jq -r '.Versions[]?, .DeleteMarkers[]? | [.Key, .VersionId] | @tsv' \
    | while IFS=$'\t' read -r KEY VERSION_ID; do
        "${AWS_CLI[@]}" s3api delete-object --bucket "$BUCKET" --key "$KEY" --version-id "$VERSION_ID" &
        ((current_jobs++))
        if (( current_jobs >= MAX_JOBS )); then
            wait -n
            ((current_jobs--))
        fi
    done
wait
