DROP TABLE IF EXISTS price_tracker_table;

CREATE TABLE price_tracker_table AS
with
compiling_market_prices_and_liquidity as ---Creating a table that allows us to monitor the price of a token_id over time, as well as its' liquidity and spread.
(
select 
question_id,
question,
tags,
outcome,
winner,
token_id,
market_id, 
asset_id,
retrieve_time,
min(case when lower(side) = 'ask' then price else NULL end) as ask_price,
max(case when lower(side) = 'bid' then price else NULL end) as bid_price,
sum(case when lower(side) = 'bid' then shares else NULL end) as bid_shares,
sum(case when lower(side) = 'ask' then shares else NULL end) as ask_shares
from public.polymarket_orders_full
group by 1,2,3,4,5,6,7,8,9
)
select
question_id,
question,
tags,
outcome,
winner,
token_id,
market_id, 
asset_id,
retrieve_time,
ask_price,
bid_price,
(ask_price - bid_price) as market_spread,
bid_shares,
ask_shares,
(ask_price+bid_price)/2 as list_price --- the avg price of the highest bid and lowest ask - we can quote the market here
from compiling_market_prices_and_liquidity
;
