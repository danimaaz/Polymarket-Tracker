DROP TABLE IF EXISTS detecting_arbitrage_table;

CREATE TABLE detecting_arbitrage_table AS
WITH gathering_outcomes_per_question AS (
    SELECT 
        question_id, 
        question,
        retrieve_time, 
        array_agg(DISTINCT outcome ORDER BY outcome ASC) AS outcomes
    FROM public.polymarket_orders_full
    GROUP BY 1,2,3
)
,
gathering_prices as --- Here, I gather the ask and bid prices per token_id, as well as join the array of total outcomes this specific question_id has for each row
(
select 
pof.question_id,
pof.question,
pof.tags,
pof.outcome,
pof.winner,
pof.token_id,
pof.market_id, 
pof.asset_id,
pof.retrieve_time,
pof.market_end_date,
gopq.outcomes,
min(case when lower(pof.side) = 'ask' then pof.price else NULL end) as ask_price
--max(case when lower(pof.side) = 'bid' then pof.price else NULL end) as bid_price,
--sum(case when lower(pof.side) = 'bid' then pof.shares else NULL end) as bid_shares,
--sum(case when lower(pof.side) = 'ask' then pof.shares else NULL end) as ask_shares
from public.polymarket_orders_full pof
join gathering_outcomes_per_question as gopq on pof.question_id = gopq.question_id and pof.retrieve_time = gopq.retrieve_time
group by 1,2,3,4,5,6,7,8,9,10,11
)
,
comparing_yes_and_no_price as (
select
question_id, 
question,
tags,
market_id, 
retrieve_time,
market_end_date,
outcomes,
max(case when lower(outcome) = lower(outcomes[1]) then ask_price else null end) as no_price,
max(case when lower(outcome) = lower(outcomes[2]) then ask_price else null end) as yes_price,
sum(case when lower(outcome) = lower(outcomes[1]) then 1 else 0 end) as no_instances,
sum(case when lower(outcome) = lower(outcomes[2]) then 1  else 0 end) as yes_instances
from gathering_prices
where outcomes[3] is null ---- this is necessary, some markets have a third outcome, like soccer markets. I am leaving these out for simplicity
group by 1,2,3,4,5,6,7)
select 
question_id, 
question,
tags,
market_id,
retrieve_time,
market_end_date,
outcomes,
yes_price,
no_price,
yes_instances,
no_instances,
case when yes_price + no_price < 1 then 1 else 0 end as is_arbitrage
from comparing_yes_and_no_price
order by 11 desc
;