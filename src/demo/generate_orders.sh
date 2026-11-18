#!/bin/sh
set -eu

DEMO_INTERVAL_SECONDS="${DEMO_INTERVAL_SECONDS:-5}"
DEMO_DELETE_EVERY="${DEMO_DELETE_EVERY:-3}"

case "$DEMO_INTERVAL_SECONDS" in
  ''|*[!0-9]*) echo "DEMO_INTERVAL_SECONDS debe ser un entero positivo" >&2; exit 2 ;;
esac
case "$DEMO_DELETE_EVERY" in
  ''|*[!0-9]*) echo "DEMO_DELETE_EVERY debe ser un entero positivo" >&2; exit 2 ;;
esac
if [ "$DEMO_DELETE_EVERY" -eq 0 ]; then
  echo "DEMO_DELETE_EVERY debe ser mayor que cero" >&2
  exit 2
fi

if [ "$DEMO_INTERVAL_SECONDS" -lt 1 ]; then
  echo "DEMO_INTERVAL_SECONDS debe ser mayor que cero" >&2
  exit 2
fi

counter=0
previous_order_id=""

echo "Generador activo: inserta y actualiza un pedido cada ${DEMO_INTERVAL_SECONDS}s; elimina uno previo cada ${DEMO_DELETE_EVERY} pedidos."
echo "Detén la demo con Ctrl+C."

while :; do
  customer_id=$((1000 + counter % 50))
  amount_cents=$((2500 + counter % 20 * 375))
  order_id="$(psql -X -qAt -v ON_ERROR_STOP=1 -c \
    "INSERT INTO public.orders (customer_id, status, amount_cents, currency) VALUES (${customer_id}, 'PENDING', ${amount_cents}, 'USD') RETURNING order_id;")"

  psql -X -q -v ON_ERROR_STOP=1 -c \
    "UPDATE public.orders SET status = 'PAID' WHERE order_id = ${order_id};" >/dev/null
  echo "Pedido ${order_id}: INSERT PENDING y UPDATE PAID"

  if [ -n "$previous_order_id" ] && [ "$((counter % DEMO_DELETE_EVERY))" -eq 0 ]; then
    psql -X -q -v ON_ERROR_STOP=1 -c \
      "DELETE FROM public.orders WHERE order_id = ${previous_order_id};" >/dev/null
    echo "Pedido ${previous_order_id}: DELETE"
  fi

  previous_order_id="$order_id"
  counter=$((counter + 1))
  sleep "$DEMO_INTERVAL_SECONDS"
done
