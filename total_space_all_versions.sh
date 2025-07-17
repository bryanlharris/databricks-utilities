#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 <aws-profile> <bucket-name> <prefix>" >&2
    exit 1
fi

PROFILE="$1"
BUCKET="$2"
PREFIX="$3"
AWS_CLI=(aws --profile "$PROFILE")

ALL_SIZE=$("${AWS_CLI[@]}" s3api list-object-versions --bucket "$BUCKET" --prefix "$PREFIX" --output json | jq '([.Versions[].Size] | add) // 0')
CURRENT_SIZE=$("${AWS_CLI[@]}" s3api list-objects-v2 --bucket "$BUCKET" --prefix "$PREFIX" --output json | jq '([.Contents[].Size] | add) // 0')
PREVIOUS_SIZE=$((ALL_SIZE - CURRENT_SIZE))

printf "Current versions total size (bytes): %s\n" "$CURRENT_SIZE"
printf "Previous versions total size (bytes): %s\n" "$PREVIOUS_SIZE"
printf "All versions total size (bytes): %s\n" "$ALL_SIZE"

