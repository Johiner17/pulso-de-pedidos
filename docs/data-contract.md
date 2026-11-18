# Contrato de datos y eventos

## Tabla fuente: `orders_source.public.orders`

| Campo | Tipo PostgreSQL | Regla |
| --- | --- | --- |
| `order_id` | `BIGINT` identity | Clave primaria estable |
| `customer_id` | `BIGINT` | Obligatorio |
| `status` | `VARCHAR(20)` | `PENDING`, `PAID`, `PROCESSING`, `SHIPPED`, `DELIVERED`, `CANCELLED` o `REFUNDED` |
| `amount_cents` | `INTEGER` | No negativo; evita números de punto flotante para dinero |
| `currency` | `CHAR(3)` | `USD` por defecto |
| `created_at` | `TIMESTAMPTZ` | Momento de creación |
| `updated_at` | `TIMESTAMPTZ` | Actualizado por trigger en cada modificación |

El conector publica `orders.public.orders`. La clave JSON contiene `order_id`; el valor es el envelope Debezium. Kafka Connect puede envolver registros JSON como `{"schema": ..., "payload": ...}` si `schemas.enable` está activo. La configuración del worker desactiva ese wrapper para mensajes nuevos; Spark admite ambas formas para tolerar cambios de configuración y registros ya existentes en Kafka. Los códigos de operación son `c` (insert), `u` (update), `d` (delete) y `r` (fila del snapshot inicial). El tombstone posterior al delete está deshabilitado para que Spark procese el evento de eliminación con su envelope.

En el primer arranque el conector usa `snapshot.mode=initial`: toma una copia consistente de las filas existentes y luego sigue el WAL. Kafka Connect guarda configuración, offsets y estado del conector en los tópicos internos `connect-configs`, `connect-offsets` y `connect-statuses`; por eso el volumen local de Kafka conserva también el avance CDC.

La inspección del tópico mostró el formato envolvente de Kafka Connect (`schema` más `payload`). El snapshot observado tenía `op="r"`, `before=null`, `after.order_id=1` y `source.snapshot="first"`. De forma simplificada:

```json
{
  "schema": { "type": "struct", "fields": "..." },
  "payload": {
    "before": null,
    "after": {
      "order_id": 1,
      "customer_id": 101,
      "status": "PENDING",
      "amount_cents": 4599,
      "currency": "USD",
      "created_at": "2026-11-18T21:06:39.419651Z",
      "updated_at": "2026-11-18T21:06:39.419651Z"
    },
    "source": { "snapshot": "first", "table": "orders" },
    "op": "r",
    "ts_ms": 1795036189109
  }
}
```

El parser extrae `payload` cuando existe y cae al objeto raíz cuando Kafka Connect está configurado para no emitir el wrapper.

## Salidas del procesador

| Capa | Ruta local Delta | Contenido |
| --- | --- | --- |
| Bronze | `data/bronze/orders_cdc` | Envelope JSON, clave, tópico, partición, offset, timestamp Kafka y batch Spark |
| Silver | `data/silver/orders_current` | Una fila por pedido vigente; un delete elimina la fila |
| Gold | `data/gold/orders_by_status` | Conteo, importe total y promedio por estado y moneda, recalculados desde Silver |
| Cuarentena | `data/quarantine/orders_cdc` | Eventos inválidos con razón y metadatos Kafka |
| Métricas | `data/observability/batch_metrics` | Eventos de entrada, válidos, eliminaciones, rechazados y retraso medio/máximo desde timestamp Debezium hasta Spark por batch |

## Reintentos y orden

- Spark mantiene offsets en su checkpoint local.
- Bronze, cuarentena y métricas usan identificadores de transacción Delta por aplicación y batch para que el reintento no vuelva a anexar sus filas.
- La primera ejecución encontró una referencia incorrecta a la vista temporal del `MERGE`. Se corrigió con una vista global calificada y se reanudó usando el mismo checkpoint y los mismos datos. El reintento confirmó los batches 0–3 sin duplicar las filas de Bronze, Silver o métricas.
- Silver aplica un `MERGE` idempotente y conserva el offset Kafka más reciente por pedido. El tópico se configura con una partición, por lo que el offset es comparable para cada clave.
- Gold se recalcula desde Silver por batch; repetir el cálculo produce el mismo resultado lógico.
- Reiniciar el procesador debe conservar `checkpoints/` y `data/`; así se recuperó el batch incompleto y se completó Gold y métricas. Si se decide empezar de cero, se deben reiniciar juntos los checkpoints y las tablas materializadas para evitar mezclar offsets nuevos con transacciones Delta anteriores.
- Kafka Connect también se reinició durante la validación. Debezium encontró el offset previo del servidor `orders`, reconoció que el snapshot inicial estaba completo, lo omitió y reanudó la lectura del WAL desde el LSN almacenado. El offset final del tópico `orders.public.orders` permaneció en `7`, igual que antes del reinicio.
- Ejecutar `spark-report` después de detener el stream ofrece una lectura coherente de las salidas; si se consulta mientras un batch está en curso, Silver puede reflejar el `MERGE` antes de que Gold y las métricas terminen de actualizarse.

Es un diseño local de un solo nodo, sin alta disponibilidad. Las semánticas descritas cubren el origen Kafka, el checkpoint de Spark y los sinks Delta locales; no equivalen a garantía de producción.
