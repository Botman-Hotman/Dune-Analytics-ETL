create or replace view data_warehouse.cow_price_improvement as
    with cow_trades as (
        select
        to_char(DATE_TRUNC('hour', cow.block_time::timestamp), 'YYYY-MM-DD HH:00') as datetime,
        cow.token_pair,
        cow.order_type,
        cow.partial_fill,
        cow.units_sold,
        cow.sell_value_usd,
        cow.sell_token,
        cow.sell_price,
        cow.units_bought,
        cow.usd_value as buy_value_usd,
        cow.buy_token,
        cow.buy_price
        from staging.cow_swap_trades cow
    )

    , coin_gecko_data as (
        select
        to_char(DATE_TRUNC('hour', geck.date_time::timestamp), 'YYYY-MM-DD HH:00') as date_time,

        case
            when geck.coin = 'ethereum' then 'WETH'
            when geck.coin = 'usd-coin' then 'USDC'
        end as coin,
        geck.prices as external_price
        from staging.coingecko_coin_data geck
    )

    , all_data as (
        select *
        from cow_trades as cow_
        left join coin_gecko_data geck_ on geck_.date_time = cow_.datetime
    )

    , buy_side as (
        select
        datetime,
        coin as external_coin,
        buy_token as cow_token,
        units_bought as amount,
        buy_price as cow_price,
        ((buy_price - external_price)/external_price) * 100 as price_difference,
        units_bought * external_price as external_value_usd,
        buy_value_usd as usd_cow_value
        from all_data where coin = buy_token
    )

    , sell_side as (
        select
        datetime,
        coin as external_coin,
        sell_token as cow_token,
        units_sold as amount,
        sell_price as cow_price,
        ((sell_price - external_price)/external_price) * 100 as price_difference,
        units_sold * external_price as external_value_usd,
        sell_value_usd as usd_cow_value
        from all_data where coin = sell_token
    )


    , final_data as (
        select * from sell_side
        union all
        select * from buy_side
    )


    select
    to_char(date_trunc('DAY', datetime::timestamp), 'YYYY-MM-DD') as date,
    cow_token,
    avg(price_difference) as avg_price_difference
    from final_data
    group by to_char(date_trunc('DAY', datetime::timestamp), 'YYYY-MM-DD'), cow_token