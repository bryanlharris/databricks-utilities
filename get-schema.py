#!/usr/bin/env python3

import sys
import platform
import argparse
import json
import subprocess
from pyspark.sql import SparkSession

if sys.platform == "darwin":
    system = "mac"
elif "microsoft" in platform.uname().release.lower():
    system = "wsl"

print(system)

parser = argparse.ArgumentParser()
parser.add_argument("--type", required=True)
parser.add_argument("--file", required=True)
parser.add_argument("--output", required=False)
parser.add_argument("--delimiter", required=False)
parser.add_argument("--multiline", action="store_true")
args = parser.parse_args()

spark = SparkSession.builder.master("local[*]") \
         .appName("edsm_bronze_load") \
         .config("spark.ui.host", "0.0.0.0") \
         .config("spark.driver.memory", "12g") \
         .config("spark.executor.memory", "12g").getOrCreate()

reader = spark.read.format(args.type)
if args.type == "csv":
    reader = reader.option("header", "true").option("inferSchema", "true")
    if args.delimiter:
        reader = reader.option("delimiter", args.delimiter)
if args.type == "json":
    if args.multiline:
        reader = reader.option("multiline", "true")
df = reader.load(args.file)

df = df.drop("_corrupt_record")
schema_json_str = df.schema.json()
schema_json = json.loads(schema_json_str)

if args.output == "stdout":
    print(json.dumps(schema_json, indent=4))
else:
    if system == "mac":
        subprocess.run("pbcopy", input=json.dumps(schema_json, indent=4).encode("utf-8"))
    elif system == "wsl":
        subprocess.run("clip.exe", input=json.dumps(schema_json, indent=4).encode("utf-8"))
