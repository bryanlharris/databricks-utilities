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

to_human() {
    numfmt --to=iec --suffix=B "$1"
}

CURRENT_HUMAN=$(to_human "$CURRENT_SIZE")
PREVIOUS_HUMAN=$(to_human "$PREVIOUS_SIZE")
ALL_HUMAN=$(to_human "$ALL_SIZE")

printf "Current versions total size: %s (%s bytes)\n" "$CURRENT_HUMAN" "$CURRENT_SIZE"
printf "Previous versions total size: %s (%s bytes)\n" "$PREVIOUS_HUMAN" "$PREVIOUS_SIZE"
printf "All versions total size: %s (%s bytes)\n" "$ALL_HUMAN" "$ALL_SIZE"

