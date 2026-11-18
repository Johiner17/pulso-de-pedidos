#!/bin/sh
set -eu

IVY_DIR="${SPARK_IVY_DIR:-/tmp/ivy-cache}"
mkdir -p "$IVY_DIR"

exec /opt/spark/bin/spark-submit --conf "spark.jars.ivy=$IVY_DIR" "$@"
