-- Scaling/performance migration

CREATE TABLE IF NOT EXISTS latest_prices (
  provider TEXT NOT NULL,
  symbol TEXT NOT NULL,
  price NUMERIC(18,8) NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  moving_average_5 NUMERIC(18,8),
  price_point_id UUID UNIQUE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (provider, symbol)
);

CREATE TABLE IF NOT EXISTS processed_price_events (
  price_point_id UUID PRIMARY KEY,
  event_id UUID NOT NULL UNIQUE,
  provider TEXT NOT NULL,
  symbol TEXT NOT NULL,
  event_timestamp TIMESTAMPTZ NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_points_ps_ts_desc_inc
ON price_points (provider, symbol, as_of DESC) INCLUDE (price);
