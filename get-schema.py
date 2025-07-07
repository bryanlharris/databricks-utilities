#!/usr/bin/env python3

import sys
import platform
import argparse
import json
import subprocess
import csv
from pathlib import Path
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession


def detect_encoding(path: str) -> str:
    """Attempt to detect the file encoding."""
    encodings = ["utf-8", "utf-16", "latin-1"]
    with open(path, "rb") as fh:
        sample = fh.read(4096)
    for enc in encodings:
        try:
            sample.decode(enc)
            return enc
        except Exception:
            continue
    return "utf-8"


def detect_csv(path: str) -> dict:
    """Return csv related options using a small sample."""
    encoding = detect_encoding(path)
    with open(path, "r", encoding=encoding, errors="ignore") as fh:
        sample = fh.read(2048)
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample)
        delimiter = dialect.delimiter
    except csv.Error:
        delimiter = ","
    has_header = sniffer.has_header(sample)
    return {"type": "csv", "delimiter": delimiter, "header": has_header, "encoding": encoding}


def detect_json(path: str) -> dict:
    """Return json related options by examining the file contents."""
    encoding = detect_encoding(path)
    # Read the entire file rather than a small sample so we can
    # distinguish between a valid JSON document and newline delimited JSON
    # (NDJSON). NDJSON will fail to load as a single JSON object or array
    # which allows us to treat it as multiline=False.
    with open(path, "r", encoding=encoding, errors="ignore") as fh:
        lines = fh.readlines()

    joined = "".join(lines)
    try:
        parsed = json.loads(joined)
        # If the file parses as a JSON object or array then it is a
        # multiline JSON file. NDJSON files will trigger the exception
        # path below which sets multiline to False.
        multiline = isinstance(parsed, (dict, list))
    except json.JSONDecodeError:
        multiline = False

    return {"type": "json", "multiline": multiline, "encoding": encoding}


def detect_file(path: str) -> dict:
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        return detect_csv(path)
    if suffix == ".txt":
        return detect_csv(path)
    if suffix == ".json":
        return detect_json(path)
    return {"type": suffix.lstrip(".")}

if sys.platform == "darwin":
    system = "mac"
elif "microsoft" in platform.uname().release.lower():
    system = "wsl"

print(system, file=sys.stderr)

parser = argparse.ArgumentParser()
parser.add_argument("--type", required=False, help="File type (csv,json,parquet)")
parser.add_argument("--file", required=True, help="Path to data file")
parser.add_argument("--output", required=False)
parser.add_argument("--delimiter", required=False, help="Override detected delimiter")
parser.add_argument("--multiline", action="store_true", help="Override detected json multiline")
args = parser.parse_args()

detected = detect_file(args.file)
if not args.type:
    args.type = detected.get("type")
print(f"Detected options: {detected}", file=sys.stderr)

spark = SparkSession.builder.master("local[*]") \
         .appName("edsm_bronze_load") \
         .config("spark.ui.host", "0.0.0.0") \
         .config("spark.driver.memory", "12g") \
         .config("spark.executor.memory", "12g").getOrCreate()

reader = spark.read.format(args.type)
if args.type == "csv":
    delimiter = args.delimiter if args.delimiter else detected.get("delimiter", ",")
    header = "true" if detected.get("header", True) else "false"
    reader = reader.option("header", header).option("inferSchema", "true").option("delimiter", delimiter)
    if detected.get("encoding"):
        reader = reader.option("encoding", detected["encoding"])
elif args.type == "json":
    multiline = args.multiline or detected.get("multiline", False)
    if multiline:
        reader = reader.option("multiline", "true")
    if detected.get("encoding"):
        reader = reader.option("encoding", detected["encoding"])
df = reader.load(args.file)

df = df.drop("_corrupt_record")
schema_json_str = df.schema.json()
schema_json = json.loads(schema_json_str)

# Always include a placeholder column for storing rescued records
schema_json.get("fields", []).append({
    "metadata": {},
    "name": "_rescued_data",
    "nullable": True,
    "type": "string",
})

result = {}
result["readStreamOptions"] = {"cloudFiles.format": args.type}
if args.type == "csv":
    result["readStreamOptions"].update({
        "delimiter": detected.get("delimiter", ","),
        "header": "true" if detected.get("header", True) else "false",
    })
    if detected.get("encoding"):
        result["readStreamOptions"]["encoding"] = detected["encoding"]
elif args.type == "json":
    result["readStreamOptions"].update({
        "multiline": "true" if args.multiline or detected.get("multiline", False) else "false",
    })
    if detected.get("encoding"):
        result["readStreamOptions"]["encoding"] = detected["encoding"]

result["file_schema"] = schema_json
result["history_schema"] = "history"

if args.output == "stdout":
    print(json.dumps(result, indent=4))
else:
    if system == "mac":
        subprocess.run("pbcopy", input=json.dumps(result, indent=4).encode("utf-8"))
    elif system == "wsl":
        subprocess.run("clip.exe", input=json.dumps(result, indent=4).encode("utf-8"))
