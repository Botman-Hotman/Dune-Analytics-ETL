import asyncio
import logging

import pandas as pd
from core.dune_analytics import dune
from core.utils import df_to_postgres_schema, df_to_abstract_orm


def init_target_data(table_name: str = 'cow_swap_trades', query_id: int = 4158553):

    query_result: pd.DataFrame = dune.get_latest_result_dataframe(query_id)

    if query_result.shape[0] > 0:
        asyncio.run(df_to_postgres_schema(query_result, table_name))  # create staging schema from existing data
        df_to_abstract_orm(query_result, table_name)

    else:
        logging.error(f"Dune Analytics returned now rows for {query_id}")


def update_target_data(table_name: str = 'cow_swap_trades', query_id: int = 4148595):
    query_result: pd.DataFrame = dune.get_latest_result_dataframe(query_id)

    if query_result.shape[0] > 0:
        df_to_abstract_orm(query_result, table_name)

    else:
        logging.error(f"Dune Analytics returned now rows for {query_id}")



if __name__ == '__main__':
    update_target_data()


