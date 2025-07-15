#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <aws-profile> <bucket-name>" >&2
    exit 1
fi

PROFILE="$1"
BUCKET="$2"
AWS_CLI=(aws --profile "$PROFILE")

# List all object versions and delete markers once and delete them in bulk.

DELETE_FILE=$(mktemp)
trap 'rm -f "$DELETE_FILE"' EXIT

"${AWS_CLI[@]}" s3api list-object-versions --bucket "$BUCKET" --output json | jq '{Objects: (.Versions + .DeleteMarkers | map({Key: .Key, VersionId: .VersionId}))}' > "$DELETE_FILE"

if [[ $(jq '.Objects | length' "$DELETE_FILE") -gt 0 ]]; then
    "${AWS_CLI[@]}" s3api delete-objects --bucket "$BUCKET" --delete "file://$DELETE_FILE"
fi
