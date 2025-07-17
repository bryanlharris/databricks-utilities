#!/usr/bin/env python3
"""Calculate S3 storage usage for all object versions under a prefix."""

from __future__ import annotations

import argparse

import boto3


def _format_size(num_bytes: int) -> str:
    """Return human readable file size for *num_bytes*."""
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sum the size of all versions of objects in a bucket prefix, "
            "including the current version."
        )
    )
    parser.add_argument("--profile", required=True, help="AWS profile to use")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument(
        "--prefix", default="", help="Prefix within the bucket to inspect"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    session = boto3.Session(profile_name=args.profile)
    s3 = session.client("s3")

    paginator = s3.get_paginator("list_object_versions")

    total_current = 0
    total_previous = 0

    for page in paginator.paginate(Bucket=args.bucket, Prefix=args.prefix):
        for version in page.get("Versions", []):
            size = version.get("Size", 0)
            if version.get("IsLatest"):
                total_current += size
            else:
                total_previous += size

    print(
        f"Current versions: {_format_size(total_current)} "
        f"({total_current} bytes)"
    )
    print(
        f"Previous versions: {_format_size(total_previous)} "
        f"({total_previous} bytes)"
    )
    combined = total_current + total_previous
    print(f"Total: {_format_size(combined)} ({combined} bytes)")


if __name__ == "__main__":
    main()
