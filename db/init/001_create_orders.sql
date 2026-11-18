CREATE TABLE IF NOT EXISTS public.orders (
    order_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id    BIGINT NOT NULL,
    status         VARCHAR(20) NOT NULL
                   CHECK (status IN ('PENDING', 'PAID', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'REFUNDED')),
    amount_cents   INTEGER NOT NULL CHECK (amount_cents >= 0),
    currency       CHAR(3) NOT NULL DEFAULT 'USD',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON public.orders (updated_at);

CREATE OR REPLACE FUNCTION public.set_order_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_orders_updated_at
BEFORE UPDATE ON public.orders
FOR EACH ROW
EXECUTE FUNCTION public.set_order_updated_at();

ALTER TABLE public.orders REPLICA IDENTITY FULL;

-- Credencial didáctica del conector, solo para este entorno local.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'local_cdc_only';
DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO debezium', current_database());
END
$$;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON TABLE public.orders TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

CREATE PUBLICATION orders_publication FOR TABLE public.orders;
