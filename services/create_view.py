from core.db import sync_engine
from sqlalchemy import text
import logging


def create_views():
    logging.info('Creating views in data warehouse')
    with sync_engine.begin() as conn:
        with open('sql/view_cow_price_improvement.sql', 'r') as sql_file:
            statement = sql_file.read()
            conn.execute(text(statement))
        conn.commit()
        conn.close()
        logging.info('ETL data warehouse ready! Check the database')