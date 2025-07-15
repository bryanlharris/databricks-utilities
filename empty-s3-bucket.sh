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

TMP_DIR=$(mktemp -d /tmp/delete.XXXXXX)
trap 'rm -rf "$TMP_DIR"' EXIT

"${AWS_CLI[@]}" s3api list-object-versions --bucket "$BUCKET" --output json |
jq -c '.Versions + .DeleteMarkers
    | map({Key: .Key} + (if .VersionId == null or .VersionId == "null" then {} else {VersionId: .VersionId} end))
    | [range(0; length; 999)] as $idxs
    | $idxs[] as $i
    | {Objects: .[$i:$i+999]}' |
while IFS= read -r CHUNK; do
    CHUNK_FILE=$(mktemp /tmp/delete_chunk.XXXXXX)
    echo "$CHUNK" > "$CHUNK_FILE"
    if [[ $(jq '.Objects | length' "$CHUNK_FILE") -gt 0 ]]; then
        "${AWS_CLI[@]}" s3api delete-objects --bucket "$BUCKET" --delete "file://$CHUNK_FILE"
    fi
    rm -f "$CHUNK_FILE"
done
