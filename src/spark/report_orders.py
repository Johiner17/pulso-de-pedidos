"""Print local analytical outputs for review and portfolio demonstrations."""

import os
from pathlib import Path

from pyspark.sql import SparkSession


TABLES = [
    ("SILVER — pedidos vigentes", os.getenv("SILVER_PATH", "/opt/data/silver/orders_current")),
    ("GOLD — pedidos por estado y moneda", os.getenv("GOLD_PATH", "/opt/data/gold/orders_by_status")),
    ("MÉTRICAS — lotes", os.getenv("METRICS_PATH", "/opt/data/observability/batch_metrics")),
    ("CUARENTENA — eventos rechazados", os.getenv("DLQ_PATH", "/opt/data/quarantine/orders_cdc")),
]


def main() -> None:
    spark = (
        SparkSession.builder.appName("pulso-orders-report")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    for title, path in TABLES:
        print(f"\n{title}\n{path}")
        if not Path(path).exists():
            print("Todavía no hay datos.")
            continue
        spark.read.format("delta").load(path).show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
