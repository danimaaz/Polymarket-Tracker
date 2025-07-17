DROP TABLE IF EXISTS polymarket_orders_full;

CREATE TABLE polymarket_orders_full AS

with unnested_polymarket_data as 
----Grabbing the full polymarket data, while unnesting the tokens jsonb -> I am unnesting it because I want to retrieve the individual information about each token (i.e. the outcome, token_id, etc) and use that info when joining with the orderbook
(select 
accepting_order_timestamp, 
accepting_orders, 
active, 
archived, 
closed, 
condition_id, 
description, 
enable_order_book, 
end_date_iso, 
fpmm, 
game_start_time, 
icon, 
image, 
is_50_50_outcome, 
maker_base_fee, 
market_slug, 
minimum_order_size, 
minimum_tick_size, 
neg_risk, 
neg_risk_market_id, 
neg_risk_request_id, 
notifications_enabled,
question, 
question_id, 
rewards, 
seconds_delay, 
tags, 
taker_base_fee, 
token_outcome, 
token_price, 
token_token_id, 
token_winner,
token_obj ->> 'price' AS price,
token_obj ->> 'winner' AS winner,
token_obj ->> 'outcome' AS outcome,
token_obj ->> 'token_id' AS token_id

from public.polymarket_market_data_full as t,
  jsonb_array_elements(t.tokens) AS token_obj
),
joining_data_with_nested_orderbook_data as 
---- Joining the polymarket data with the orderbooks data (With the orderbooks data still being in a 'nest')
(
select 
upd.question_id,
upd.question,
upd.tags,
upd.outcome,
upd.winner,
upd.token_id,
odf.market_id,
odf.asset_id,
to_timestamp(odf.timestamp_unix) as orderbook_timestamp,
to_timestamp(retrieve_time) as retrieve_time,
to_timestamp(end_date_iso*1000) as market_end_date,
odf.side,
odf.orderbook
from public.orderbooks_data_full as odf 
join unnested_polymarket_data as upd on upd.token_id = odf.token_id --- using the orderbooks data as the main table here because each token has two rows - bid side / ask side. to avoid confusion
)

---- Unnesting the full orderbook, each step of the orderbook will have its own row

select
jdno.question_id,
jdno.question,
jdno.tags,
jdno.outcome,
jdno.winner,
jdno.token_id,
jdno.market_id,
jdno.asset_id,
jdno.orderbook_timestamp,
jdno.retrieve_time,
jdno.market_end_date,
jdno.side,
order_entry.key::float8 as price,
order_entry.value::float8 as shares
from joining_data_with_nested_orderbook_data as jdno,
LATERAL jsonb_each(jdno.orderbook) AS order_entry ---using the lateral function to unnest the full orderbook
order by orderbook_timestamp desc
;
