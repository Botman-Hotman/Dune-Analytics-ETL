

create or replace view data_warehouse.cow_aggregation_data as 
select 
block_date,
token_pair,
count(distinct tx_hash) no_txn,
sum(usd_value) as total_usd_value_bought,
sum(sell_value_usd) as total_usd_value_sold,
sum(units_sold) as total_units_sold,
sum(units_bought) as total_units_bought,
avg(buy_price) as avg_buy_price,
avg(sell_price) as avg_sell_price

from staging.cow_swap_trades cst 
group by block_date, token_pair