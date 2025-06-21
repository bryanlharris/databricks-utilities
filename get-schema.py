#!/usr/bin/env python3

import argparse
import json
import subprocess
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--type", required=True)
parser.add_argument("--file", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()
df = spark.read.format(args.type).load(args.file)
schema_json_str = df.schema.json()
schema_json = json.loads(schema_json_str)
subprocess.run("clip.exe", input=json.dumps(schema_json, indent=4).encode("utf-8"))
