import logging
import time
import pandas as pd

from dune_client.query import QueryBase, QueryParameter
from core.dune_analytics import dune, cyrpto_api_settings
from core.utils import df_to_postgres_schema, df_to_abstract_orm, settings
from celery_worker import celery_app


async def init_dune_data(
        table_name: str = 'cow_swap_trades',
        query_id: int = cyrpto_api_settings.dune_analytics_init_query_id,
        target_buy_token: str = 'WETH',
        target_sell_token: str = 'USDC'
) -> None | dict:
    """
    Fetch all data from target dune query and create a staging table based on the data pulled.
    :param target_buy_token:
    :param target_sell_token:
    :param table_name:
    :param query_id:
    :return:
    """
    target_query = QueryBase(
        query_id=query_id,
        params=[
            QueryParameter.text_type('target_buy_token', target_buy_token)
            , QueryParameter.text_type('target_sell_token', target_sell_token)
        ]
    )

    query_result: pd.DataFrame = dune.run_query_dataframe(target_query, ping_frequency=30)
    df_len: int = query_result.shape[0]

    if df_len > 0:
        logging.info(f"inserting {df_len} rows of {target_buy_token}-{target_sell_token} from query_id {query_id} into {table_name} staging")
        await df_to_postgres_schema(query_result, table_name)  # create staging schema from existing data
        time.sleep(1)
        df_to_abstract_orm(query_result, table_name)
        return {
            "schema": settings.staging_schema,
            "table_name": table_name,
            "query_id": query_id,
            "total_rows": df_len,
            "columns": query_result.columns
        }

    else:
        logging.error(f"Dune Analytics returned now rows for {query_id}")
        return


@celery_app.task()
def update_dune_data(
        table_name: str = 'cow_swap_trades',
        query_id: int = cyrpto_api_settings.dune_analytics_update_query_id,
        target_buy_token: str = 'WETH',
        target_sell_token: str = 'USDC'
) -> None | dict:
    """
    Fetch all data from target dune query and create a staging table based on the data pulled.
    :param target_buy_token:
    :param target_sell_token:
    :param table_name:
    :param query_id:
    :return:
    """
    target_query = QueryBase(
        query_id=query_id,
        params=[
            QueryParameter.text_type('target_buy_token', target_buy_token)
            , QueryParameter.text_type('target_sell_token', target_sell_token)
        ]
    )

    query_result: pd.DataFrame = dune.run_query_dataframe(target_query, ping_frequency=30)
    df_len: int = query_result.shape[0]

    if df_len > 0:
        logging.info(f"updating {df_len} rows of {target_buy_token}-{target_sell_token} from query_id {query_id} into {table_name} staging")
        df_to_abstract_orm(query_result, table_name)
        return {
            "schema": settings.staging_schema,
            "table_name": table_name,
            "query_id": query_id,
            "total_rows": df_len,
            "columns": query_result.columns
        }

    else:
        logging.error(f"Dune Analytics returned now rows for {target_buy_token}-{target_sell_token} with query_id {query_id}")
        return


if __name__ == '__main__':
    update_dune_data()


