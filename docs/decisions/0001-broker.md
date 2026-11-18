# ADR 0001: broker Kafka para el MVP local

- Estado: aceptada
- Fecha: 2026-11-18

## Contexto

El pipeline necesita un broker compatible con Kafka para recibir los eventos del conector Debezium y permitir que Spark Structured Streaming consuma esos eventos. Debe poder ejecutarse localmente y no requerir una suscripción o servicio cloud.

## Decisión

Usar **Apache Kafka en modo KRaft** para el MVP.

## Motivos

- Kafka es el protocolo y ecosistema de streaming que queremos demostrar directamente.
- Apache publica una imagen Docker oficial y guías para ejecutar Kafka en modo KRaft sin ZooKeeper.
- Debezium se instala en Kafka Connect y publica cambios de PostgreSQL en tópicos Kafka.
- El uso local evita cargos por Amazon MSK u otros brokers administrados.
- Se reduce la ambigüedad sobre compatibilidad de APIs entre el broker y los clientes.

## Alternativas consideradas

### Redpanda

Redpanda sigue ofreciendo una edición comunitaria gratuita con un límite publicado de 50 cores, y sus guías aún documentan despliegue local en Docker. Sin embargo, el 12 de noviembre de 2026 anunció que desde 26.3.1 el motor dejará de ser source-available y archivó su repositorio principal. No está descontinuado, pero Apache Kafka ofrece una base más directa y menos dependiente de los cambios de distribución para este portafolio.

### Amazon MSK

No se elige para el MVP: requiere servicios reales de AWS y tiene cobro por clúster, particiones, datos y almacenamiento.

## Consecuencias

- Se deberá configurar el nodo Kafka y sus listeners para acceso desde contenedores de Compose y desde el host si hace falta.
- Kafka Connect se ejecutará con el plugin Debezium PostgreSQL.
- Las imágenes y versiones se fijarán explícitamente una vez comprobada la compatibilidad y descargabilidad del conjunto.
- No se desplegará un cluster de Kafka de producción; el objetivo local es didáctico e integrativo.

## Referencias

- [Descargas e imagen Docker oficial de Apache Kafka](https://kafka.apache.org/downloads/)
- [Guía Docker de Apache Kafka](https://kafka.apache.org/44/getting-started/docker/)
- [Arquitectura del conector PostgreSQL de Debezium](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Versiones probadas de Debezium 3.7 y Kafka Connect](https://debezium.io/installation/kafka-connect/)
- [Anuncio de Redpanda sobre R2 y distribución desde 26.3.1](https://www.redpanda.com/blog/r2-the-engine-of-engines)
