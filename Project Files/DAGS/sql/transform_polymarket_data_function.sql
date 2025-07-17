DROP TABLE IF EXISTS polymarket_market_data_full;

CREATE TABLE polymarket_market_data_full AS
SELECT
  CAST(
    CASE
      WHEN accepting_order_timestamp ~ '^[0-9]+(\.[0-9]+)?$'
      THEN CAST(accepting_order_timestamp::float AS bigint)
      ELSE NULL
    END AS bigint
  ) AS accepting_order_timestamp,
  accepting_orders::boolean,
  active::boolean,
  archived::boolean,
  closed::boolean,
  condition_id::varchar,
  description::text,
  enable_order_book::boolean,
  CAST(
    CASE
      WHEN end_date_iso ~ '^[0-9]+(\.[0-9]+)?$'
      THEN CAST(end_date_iso::float AS bigint)
      ELSE NULL
    END AS bigint
  ) AS end_date_iso,
  fpmm::varchar,
  game_start_time::varchar,
  icon::text,
  image::text,
  is_50_50_outcome::boolean,
  maker_base_fee::int4,
  market_slug::text,
  minimum_order_size::int4,
  minimum_tick_size::float4,
  neg_risk::boolean,
  neg_risk_market_id::text,
  neg_risk_request_id::text,
  notifications_enabled::boolean,
  question::text,
  question_id::varchar,
  rewards::jsonb,
  seconds_delay::int4,
  tags::_text,
  taker_base_fee::int4,
  token_outcome::varchar,
  token_price::varchar,
  token_token_id::varchar,
  token_winner::varchar,
  tokens::jsonb
FROM polymarket_market_data_raw;

-- Same for orderbooks_data_full
DROP TABLE IF EXISTS orderbooks_data_full;

CREATE TABLE orderbooks_data_full AS
SELECT
  token_id,
  market as market_id,
  asset_id,
  timestamp_unix::bigint,
  retrieve_time::bigint,
  side,
  orderbook::jsonb,
  hash
FROM orderbooks_data_raw;

DROP TABLE IF EXISTS binance_orderbook_full;

CREATE TABLE binance_orderbook_full AS
SELECT
  currency_pair::text,
  retrieve_time::bigint,
  side::text,
  price::float,
  volume::float
FROM binance_orderbook_raw;
