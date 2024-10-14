import argparse
import asyncio
import logging
from logging.handlers import RotatingFileHandler

from core.db import async_session_factory, drop_all_schema, create_all_schema, settings
from core.utils import SchemaInit, populate_dimensions_on_startup
from services import init_coingecko_prices, init_dune_data
from services.create_view import create_views

logging.basicConfig(
    format='%(asctime)s : %(name)s :  %(levelname)s : %(funcName)s :  %(message)s'
    , level=logging.DEBUG if settings.debug_logs else logging.INFO
    , handlers=[
        RotatingFileHandler(
            filename=f"logs/{settings.app_name}.log",
            maxBytes=10 * 1024 * 1024,  # 10 MB per file,
            backupCount=7  # keep 7 backups
        ),
        logging.StreamHandler()  # Continue to log to the console as well
    ]
)

# Create arg parser object
parser = argparse.ArgumentParser()
parser.add_argument("--start_date", help="Look back period to init database from in YYYY-MM-DD format, if blank will default to first day of the month", required=False)


async def init_database(start_date: str):
    if settings.init_db:
        logging.info("Starting database initialisation.")
        async with async_session_factory() as session:
            logging.info(f"Creating schema for {settings.dw_schema}.")
            await SchemaInit().create_schema(session, settings.staging_schema)
            await SchemaInit().create_schema(session, settings.dw_schema)

        if settings.dev:
            logging.info("Dropping all schemas for dev environment.")
            await drop_all_schema()  # drop the schema for dev work

        logging.info("Creating all schemas.")
        await create_all_schema()

        async with async_session_factory() as session:
            logging.info("Populating dimensions on startup.")
            await populate_dimensions_on_startup(session)

        from datetime import datetime
        if start_date is None or start_date == '':
            start_date = datetime.now().replace(day=1).strftime('%Y-%m-%d')

        # fetch all the required data to start db
        logging.info(f"Fetching all init data required from {start_date} till yesterday.")
        await init_coingecko_prices('ethereum', start_date)
        await init_coingecko_prices('usd-coin', start_date)
        await init_dune_data()
        create_views()
        logging.info('Initialisation complete.')

if __name__ == "__main__":
    args = parser.parse_args()

    # Run the init function asynchronously
    asyncio.run(init_database(args.start_date))
