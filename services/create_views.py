from core.db import sync_engine
from sqlalchemy import text
import logging


def create_cow_price_improvement_view():
    logging.info('Creating price_improvement view in data warehouse')
    with sync_engine.begin() as conn:
        with open('sql/view_cow_price_improvement.sql', 'r') as sql_file:
            statement = sql_file.read()
            conn.execute(text(statement))
        conn.commit()
        conn.close()


def create_cow_aggregation_data_view():
    logging.info('Creating aggregation_data view in data warehouse')
    with sync_engine.begin() as conn:
        with open('sql/view_cow_aggregation_data.sql', 'r') as sql_file:
            statement = sql_file.read()
            conn.execute(text(statement))
        conn.commit()
        conn.close()