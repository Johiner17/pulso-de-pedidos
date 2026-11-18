"""Consume Debezium PostgreSQL events and maintain local Delta tables."""

import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders.public.orders")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/opt/data/bronze/orders_cdc")
SILVER_PATH = os.getenv("SILVER_PATH", "/opt/data/silver/orders_current")
GOLD_PATH = os.getenv("GOLD_PATH", "/opt/data/gold/orders_by_status")
DLQ_PATH = os.getenv("DLQ_PATH", "/opt/data/quarantine/orders_cdc")
METRICS_PATH = os.getenv("METRICS_PATH", "/opt/data/observability/batch_metrics")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/opt/checkpoints/orders_stream")

ORDER_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.LongType()),
        T.StructField("customer_id", T.LongType()),
        T.StructField("status", T.StringType()),
        T.StructField("amount_cents", T.IntegerType()),
        T.StructField("currency", T.StringType()),
        T.StructField("created_at", T.StringType()),
        T.StructField("updated_at", T.StringType()),
    ]
)

ENVELOPE_SCHEMA = T.StructType(
    [
        T.StructField("before", ORDER_SCHEMA),
        T.StructField("after", ORDER_SCHEMA),
        T.StructField(
            "source",
            T.StructType(
                [
                    T.StructField("version", T.StringType()),
                    T.StructField("connector", T.StringType()),
                    T.StructField("name", T.StringType()),
                    T.StructField("ts_ms", T.LongType()),
                    T.StructField("snapshot", T.StringType()),
                    T.StructField("db", T.StringType()),
                    T.StructField("schema", T.StringType()),
                    T.StructField("table", T.StringType()),
                    T.StructField("txId", T.LongType()),
                    T.StructField("lsn", T.LongType()),
                ]
            ),
        ),
        T.StructField("op", T.StringType()),
        T.StructField("ts_ms", T.LongType()),
    ]
)

KEY_SCHEMA = T.StructType([T.StructField("order_id", T.LongType())])

STATE_COLUMNS = [
    "order_id",
    "customer_id",
    "status",
    "amount_cents",
    "currency",
    "created_at",
    "updated_at",
    "last_event_partition",
    "last_event_offset",
    "last_event_op",
]


def append_delta_once(dataframe, path: str, app_id: str, batch_id: int) -> None:
    """Use Delta transaction identifiers to make micro-batch appends retry-safe."""
    (
        dataframe.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
        .save(path)
    )


def create_current_state_table(spark: SparkSession) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS delta.`{SILVER_PATH}` (
            order_id BIGINT,
            customer_id BIGINT,
            status STRING,
            amount_cents INT,
            currency STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            last_event_partition INT,
            last_event_offset BIGINT,
            last_event_op STRING
        ) USING DELTA
        """
    )


def process_microbatch(spark: SparkSession, kafka_batch, batch_id: int) -> None:
    if kafka_batch.isEmpty():
        return

    parsed = (
        kafka_batch.select(
            F.col("key").cast("string").alias("key_json"),
            F.col("value").cast("string").alias("payload_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        # Kafka Connect may wrap JSON records as {schema, payload}; keep compatibility
        # with both that format and schema-less records when old/new records coexist.
        .withColumn(
            "payload_content",
            F.coalesce(F.get_json_object("payload_json", "$.payload"), F.col("payload_json")),
        )
        .withColumn(
            "key_content",
            F.coalesce(F.get_json_object("key_json", "$.payload"), F.col("key_json")),
        )
        .withColumn("envelope", F.from_json("payload_content", ENVELOPE_SCHEMA))
        .withColumn("key", F.from_json("key_content", KEY_SCHEMA))
        .withColumn("op", F.col("envelope.op"))
        .withColumn(
            "order_id",
            F.coalesce(
                F.col("envelope.after.order_id"),
                F.col("envelope.before.order_id"),
                F.col("key.order_id"),
            ),
        )
    )

    total_events = parsed.count()
    valid_ops = ["c", "u", "d", "r"]
    valid_statuses = ["PENDING", "PAID", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED"]
    is_delete = F.col("op") == "d"
    valid_after_image = (
        F.col("envelope.after.order_id").isNotNull()
        & F.col("envelope.after.customer_id").isNotNull()
        & F.col("envelope.after.status").isin(valid_statuses)
        & F.col("envelope.after.amount_cents").isNotNull()
        & (F.col("envelope.after.amount_cents") >= 0)
        & (F.length(F.trim(F.col("envelope.after.currency"))) == 3)
    )
    valid_condition = F.coalesce(
        F.col("op").isin(valid_ops)
        & F.col("order_id").isNotNull()
        & (is_delete | valid_after_image),
        F.lit(False),
    )
    valid = parsed.filter(valid_condition)
    rejected = parsed.filter(~valid_condition)
    valid_count = valid.count()
    rejected_count = rejected.count()
    delete_count = valid.filter(F.col("op") == "d").count()
    lag_stats = (
        parsed.select(
            (F.unix_millis(F.current_timestamp()) - F.col("envelope.ts_ms")).alias("lag_ms")
        )
        .agg(F.avg("lag_ms").alias("avg_lag_ms"), F.max("lag_ms").alias("max_lag_ms"))
        .first()
    )
    avg_lag_ms = int(lag_stats["avg_lag_ms"]) if lag_stats["avg_lag_ms"] is not None else None
    max_lag_ms = int(lag_stats["max_lag_ms"]) if lag_stats["max_lag_ms"] is not None else None

    bronze = parsed.select(
        "key_json",
        "payload_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.lit(batch_id).cast("long").alias("spark_batch_id"),
        F.current_timestamp().alias("ingested_at"),
    )
    append_delta_once(bronze, BRONZE_PATH, "pulso-orders-bronze-v1", batch_id)

    if rejected_count:
        dead_letter = rejected.select(
            "key_json",
            "payload_json",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            F.when(F.col("envelope").isNull(), F.lit("invalid_json_or_envelope"))
            .when(F.col("op").isNull(), F.lit("missing_operation"))
            .when(~F.col("op").isin(valid_ops), F.concat(F.lit("unsupported_operation:"), F.col("op")))
            .when(F.col("order_id").isNull(), F.lit("missing_order_id"))
            .otherwise(F.lit("invalid_order_fields"))
            .alias("rejection_reason"),
            F.lit(batch_id).cast("long").alias("spark_batch_id"),
            F.current_timestamp().alias("quarantined_at"),
        )
        append_delta_once(dead_letter, DLQ_PATH, "pulso-orders-dlq-v1", batch_id)

    order_changes = valid.select(
        "order_id",
        F.col("envelope.after.customer_id").alias("customer_id"),
        F.col("envelope.after.status").alias("status"),
        F.col("envelope.after.amount_cents").alias("amount_cents"),
        F.trim(F.col("envelope.after.currency")).alias("currency"),
        F.to_timestamp(F.col("envelope.after.created_at")).alias("created_at"),
        F.to_timestamp(F.col("envelope.after.updated_at")).alias("updated_at"),
        F.col("kafka_partition").alias("last_event_partition"),
        F.col("kafka_offset").alias("last_event_offset"),
        F.col("op").alias("last_event_op"),
    )

    # One partition per topic keeps the offset order comparable for each order_id.
    latest_per_order = (
        order_changes.withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy("order_id").orderBy(F.col("last_event_offset").desc())
            ),
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    latest_per_order.createOrReplaceGlobalTempView("order_change_batch")
    create_current_state_table(spark)

    target = f"delta.`{SILVER_PATH}`"
    assignments = ",\n".join(f"target.{column} = source.{column}" for column in STATE_COLUMNS[1:])
    insert_columns = ", ".join(STATE_COLUMNS)
    insert_values = ", ".join(f"source.{column}" for column in STATE_COLUMNS)
    spark.sql(
        f"""
        MERGE INTO {target} AS target
        USING global_temp.order_change_batch AS source
          ON target.order_id = source.order_id
        WHEN MATCHED
          AND source.last_event_offset > target.last_event_offset
          AND source.last_event_op = 'd'
          THEN DELETE
        WHEN MATCHED
          AND source.last_event_offset > target.last_event_offset
          AND source.last_event_op <> 'd'
          THEN UPDATE SET {assignments}
        WHEN NOT MATCHED AND source.last_event_op <> 'd'
          THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
    )
    spark.catalog.dropGlobalTempView("order_change_batch")

    current_orders = spark.read.format("delta").load(SILVER_PATH)
    gold = current_orders.groupBy("status", "currency").agg(
        F.count("*").alias("active_orders"),
        F.sum("amount_cents").alias("gross_amount_cents"),
        F.round(F.avg("amount_cents"), 2).alias("average_amount_cents"),
    ).withColumn("refreshed_at", F.current_timestamp())
    gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_PATH)

    metric_row = spark.createDataFrame(
        [
            (
                int(batch_id),
                datetime.now(timezone.utc).replace(tzinfo=None),
                int(total_events),
                int(valid_count),
                int(delete_count),
                int(rejected_count),
                avg_lag_ms,
                max_lag_ms,
            )
        ],
        schema="spark_batch_id LONG, processed_at TIMESTAMP, source_events LONG, valid_events LONG, delete_events LONG, rejected_events LONG, avg_debezium_to_spark_lag_ms LONG, max_debezium_to_spark_lag_ms LONG",
    )
    append_delta_once(metric_row, METRICS_PATH, "pulso-orders-metrics-v1", batch_id)

    print(
        json.dumps(
            {
                "spark_batch_id": batch_id,
                "source_events": total_events,
                "valid_events": valid_count,
                "delete_events": delete_count,
                "rejected_events": rejected_count,
                "avg_debezium_to_spark_lag_ms": avg_lag_ms,
                "max_debezium_to_spark_lag_ms": max_lag_ms,
            }
        ),
        flush=True,
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("pulso-orders-cdc")
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", "500")
        .load()
    )

    query = (
        kafka_stream.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_microbatch(spark, batch_df, batch_id)
        )
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="5 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
