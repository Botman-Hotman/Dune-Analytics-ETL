import asyncio
import logging
import time

import pandas as pd
from datetime import datetime, timedelta
from celery_worker import celery_app
from core.coingecko import authentication, get_historical_price
from core.utils import convert_unix_to_utc, df_to_abstract_orm, df_to_postgres_schema


async def init_coingecko_prices(target_coin: str, target_start_date: str, table_name: str = 'coingecko_coin_data') -> None | bool:
    """
    Fetches all data from the target start date to yesterday and inserts into database.
    :param target_coin:
    :param target_start_date:
    :param table_name:
    :return:
    """
    if authentication():
        time.sleep(1)
        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        target_data: dict = get_historical_price(target_coin, f"{target_start_date} 00:00:00", f"{yesterday_date} 23:59:59")

        if target_data is not None:
            # convert unix to utc
            df = pd.DataFrame()
            df['date_time'] = [convert_unix_to_utc(item[0]) for item in target_data['prices']]
            df['prices'] = [price[1] for price in target_data['prices']]
            df['market_cap'] = [price[1] for price in target_data['market_caps']]
            df['total_volume'] = [price[1] for price in target_data['total_volumes']]
            df['coin'] = target_coin

            if df.shape[0] > 0:
                logging.info(f"inserting {target_coin} {df.shape[0]} rows into {table_name} staging")
                await df_to_postgres_schema(df, table_name)  # create staging schema from existing data
                time.sleep(1)
                df_to_abstract_orm(df, table_name)

                return True

            else:
                logging.warning(f'No rows to insert into {table_name} staging table for {target_coin} | {target_start_date} params')
                return

        else:
            logging.warning(f"target params returned no data {target_coin} | {target_start_date}")
            return

    else:
        logging.error(f"coingecko failed to authenticate, stopping.")
        return


@celery_app.task()
def update_coingecko_prices(target_coin: str,
                            target_date: str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                            table_name: str = 'coingecko_coin_data') -> None:
    if authentication():
        time.sleep(1)

        target_data: dict = get_historical_price(target_coin, f"{target_date} 00:00:00", f"{target_date} 23:59:59")

        if target_data is not None:
            # convert unix to utc
            df = pd.DataFrame()
            df['date_time'] = [convert_unix_to_utc(item[0]) for item in target_data['prices']]
            df['prices'] = [price[1] for price in target_data['prices']]
            df['market_cap'] = [price[1] for price in target_data['market_caps']]
            df['total_volume'] = [price[1] for price in target_data['total_volumes']]
            df['coin'] = target_coin

            if df.shape[0] > 0:
                logging.info(f"updating {target_coin} {df.shape[0]} rows into {table_name} staging")
                df_to_abstract_orm(df, table_name)

            else:
                logging.warning(f'No rows to insert into {table_name} staging table for {target_coin} | {target_date} params')
                return

        else:
            logging.warning(f"target params returned no data {target_coin} | {target_date}")
            return

    else:
        logging.error(f"coingecko failed to authenticate, stopping.")
        return


if __name__ == '__main__':
    update_coingecko_prices('ethereum', '2024-10-11')
