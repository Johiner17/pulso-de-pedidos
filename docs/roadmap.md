# Hoja de ruta

## Fase 0 — alcance y decisiones

- [x] Definir el proyecto como pipeline local de CDC y streaming.
- [x] Definir costo cero como requisito del MVP.
- [x] Separar AWS Emulator de un despliegue real en AWS.
- [x] Definir tabla `orders`, importes en centavos y estados permitidos.
- [x] Elegir PostgreSQL 16, Apache Kafka 4.3.1 y Debezium 3.7.1.Final como base documentada.
- [x] Fijar Spark 4.2.0, Kafka source connector 4.2.0 y Delta Lake 4.4.1 según matriz de compatibilidad.
- [x] Revisar sintaxis declarativa del Compose y configuración JSON del conector.
- [x] Confirmar ejecución del Compose en el entorno Docker local.

## Fase 1 — fuente y broker

- [x] Crear Compose inicial para PostgreSQL, Kafka KRaft y Kafka Connect/Debezium.
- [x] Habilitar configuración de PostgreSQL necesaria para CDC.
- [x] Crear esquema, publicación PostgreSQL y datos semilla reproducibles.
- [x] Añadir un generador local repetible de inserciones, actualizaciones y eliminaciones.
- [x] Confirmar conectividad Kafka y crear el tópico.

## Fase 2 — CDC con Debezium

- [x] Añadir Kafka Connect con Debezium PostgreSQL y plantilla de configuración del conector.
- [x] Documentar snapshot inicial, offsets y tópicos internos de Kafka Connect.
- [x] Confirmar el estado `RUNNING` del conector y la tarea.
- [x] Inspeccionar un evento real del snapshot y ajustar el contrato/parser al wrapper JSON observado.
- [x] Demostrar eventos de inserción, actualización y eliminación.
- [x] Verificar en ejecución la reanudación del conector desde sus offsets persistidos; Debezium omitió el snapshot ya completado y retomó el LSN guardado.

## Fase 3 — procesamiento streaming

- [x] Añadir Spark Structured Streaming con conectores Kafka y Delta compatibles.
- [x] Parsear el envelope JSON de Debezium y separar operaciones válidas de rechazadas.
- [x] Guardar el log de eventos con checkpoint y transacciones Delta reintentables.
- [x] Construir estado vigente, agregación analítica y métricas por batch.
- [x] Definir deduplicación por clave dentro del batch, idempotencia por offset, eliminaciones y recuperación.
- [x] Usar una caché Ivy local escribible para resolver los JARs Maven desde la imagen oficial de Spark.
- [x] Confirmar que el primer batch leyó y guardó los tres eventos del snapshot en Bronze.
- [x] Confirmar ejecución del job streaming con Docker Desktop.

## Fase 4 — calidad y observabilidad

- [x] Añadir reglas de calidad y salida de cuarentena.
- [x] Medir volumen, errores, retraso Debezium a Spark y resultado por micro-batch.
- [x] Crear una demostración de recuperación tras reinicio, reanudando con el checkpoint conservado.
- [x] Añadir consultas de verificación y datos esperados mediante `spark-report`.

## Fase 5 — AWS Emulator (sin cargos cloud)

- [x] Revisar las modalidades oficiales, el costo de servicio nulo, la pérdida de estado al apagarlo y la telemetría predeterminada.
- [x] Confirmar con la prueba local que las seis operaciones S3 elegidas funcionan en la versión fijada; no inferir paridad general con AWS.
- [x] Preparar un perfil aislado, endpoint solo en loopback, telemetría desactivada y credenciales generadas localmente.
- [x] Crear una prueba S3 desechable con Boto3 para el flujo de bucket y objetos.
- [x] Construir la imagen local y ejecutar la prueba S3 en Docker; las seis operaciones del smoke test terminaron en `PASS`.
- [x] Documentar claramente qué se emuló y las limitaciones: flujo S3 concreto probado localmente, recursos efímeros y sin afirmar paridad completa con AWS.

## Fase 6 — presentación de portafolio

- [x] Completar README con arquitectura, inicio, demo, decisiones y limitaciones.
- [x] Crear una guía explicativa del proyecto, su ejecución y su narrativa de entrevista.
- [x] Añadir diagramas de arquitectura en README y documentación.
- [ ] Añadir capturas de las ejecuciones locales para la presentación visual.
- [x] Documentar que una traducción a AWS real requiere revisar tarifas, créditos y límites antes de crear recursos.
- [ ] Considerar una prueba cloud mínima solo después de revisar créditos y precios.
