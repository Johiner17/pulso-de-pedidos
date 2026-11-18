# ADR 0002: Spark Structured Streaming y Delta Lake

- Estado: aceptada
- Fecha: 2026-11-18

## Contexto

El procesador debe leer eventos Kafka, conservar el envelope CDC, aplicar inserts/updates/deletes al estado vigente, recalcular agregaciones y reintentar micro-batches sin duplicar anexos. El entorno se ejecuta localmente sin servicios cloud.

## Decisión

Usar Apache Spark 4.2.0 en la imagen oficial `apache/spark:4.2.0-python3`, el conector `spark-sql-kafka-0-10_2.13:4.2.0` y Delta Lake 4.4.1 (`delta-spark_4.2_2.13`). La matriz oficial de Delta lista 4.4.x como compatible con Spark 4.2.x.

Bronze y cuarentena guardan eventos append-only. Silver aplica `MERGE` por `order_id`, con offset Kafka para ignorar actualizaciones repetidas o antiguas; Gold se calcula desde Silver. Las escrituras append de `foreachBatch` usan `txnAppId` y `txnVersion` según el patrón oficial de Delta.

## Consecuencias y límites

- Se requiere internet en el primer arranque para descargar imágenes y dependencias Maven.
- `run_spark.sh` crea y selecciona una caché Ivy escribible en `.cache/ivy`; la imagen oficial puede tener `/nonexistent` como directorio personal, que no sirve para el Ivy por defecto de Spark 4.
- Spark corre con un worker local y un solo topic partition para mantener comparables los offsets de cada clave en este MVP.
- Checkpoint, broker y tablas son locales; no hay alta disponibilidad.
- Si se borra el checkpoint y se conserva el mismo dataset, hay que coordinar el reinicio de offsets/transacciones Delta. El procedimiento de reinicio debe mantener juntos el checkpoint y las tablas, o rotar los identificadores de aplicación Delta según corresponda.
- El código debe verificarse con una ejecución integrada; la compatibilidad declarada de versiones no sustituye esa prueba.

## Referencias

- [Notas oficiales de Delta Lake 4.4.1: compatibilidad con Apache Spark 4.2.0](https://github.com/delta-io/delta/releases/tag/v4.4.1)
- [Notas oficiales de Debezium 3.7: probado con Kafka Connect y broker 4.3.1](https://debezium.io/releases/3.7/release-notes/)
- [Versiones compatibles de Delta Lake y Apache Spark](https://docs.delta.io/releases/)
- [Imagen oficial de Apache Spark](https://hub.docker.com/_/spark)
- [Guía de integración de Spark Structured Streaming con Kafka](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html)
- [Configuración Spark 4.2.0: `spark.jars.ivy`](https://spark.apache.org/docs/4.2.0/configuration.html)
- [Escrituras idempotentes con foreachBatch en Delta](https://docs.delta.io/delta-streaming/)
- [MERGE e idempotencia de foreachBatch](https://docs.delta.io/delta-update/)
