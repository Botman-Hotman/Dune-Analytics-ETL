import pandas as pd
import logging

from datetime import datetime

from core.db import (
    map_dtype_to_postgres,
    async_engine,
    settings,
    sync_engine,
    sync_session_factory,
    Base
)
from sqlalchemy import text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import List, Type
import hashlib
import uuid


def create_uuid_from_string(val: str):
    hex_string = hashlib.md5(val.encode("UTF-8")).hexdigest()
    return uuid.UUID(hex=hex_string)


def row_to_uuid(row):
    row_string = ','.join(row.astype(str))  # Convert each element of the row to string and join them with commas
    return create_uuid_from_string(row_string)


def convert_to_unix_timestamp(date_string):
    date_format = "%Y-%m-%d %H:%M:%S"  # Define the expected format for the input date string
    dt = datetime.strptime(date_string, date_format)  # Parse the date string into a datetime object
    unix_timestamp = int(dt.timestamp())  # Convert the datetime object to a Unix timestamp
    return unix_timestamp


def convert_unix_to_utc(unix_timestamp_ms):
    unix_timestamp = unix_timestamp_ms / 1000
    dt = datetime.utcfromtimestamp(unix_timestamp)  # Convert the Unix timestamp to a UTC datetime object
    utc_time_string = dt.strftime('%Y-%m-%d %H:%M:%S')  # Format the datetime object into a string
    return utc_time_string


async def df_to_postgres_schema(df: pd.DataFrame, table_name: str) -> None:
    """
    Create a staging table based on the pandas dataframe schema passed into the function.
    A uuid id column is generated that is used to remove duplicates of the imported data.
    :param table_name:
    :param df:
    :return:
    """
    async with async_engine.begin() as conn:
        table_exists_query = f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = '{settings.staging_schema}' 
                                AND table_name = '{table_name}'
                            );
                        """
        result = await conn.execute(text(table_exists_query))
        table_exists = result.scalar()

        if not table_exists:
            # map pandas data column types into postgres
            df.columns = [f"column_{i}" if col.startswith('Unnamed') else col.strip().replace(' ', '_').lower() for i, col in enumerate(df.columns)]
            column_definitions: list = [f""""{col}" {map_dtype_to_postgres(df[col].dtype)}""" for col in df.columns]

            # Add 'id' column for UUID as primary key
            column_definitions.insert(0, '"id" UUID PRIMARY KEY')

            # Create table if it doesn't exist, or ensure structure if it does
            create_table_query = f"""CREATE TABLE IF NOT EXISTS {settings.staging_schema}."{table_name}" ({", ".join(column_definitions)});"""
            await conn.execute(text(create_table_query))
            await conn.commit()


def df_to_abstract_orm(df: pd.DataFrame, table_name: str) -> None:
    """
    Convert pandas dataframe into an abstract orm object, generate a uuid 'id' column based on the row data, and insert into a staging area.
    Used in conjunction with df_to_postgres_schema function.
    :param table_name:
    :param df:
    :return:
    """
    metadata = MetaData(schema=settings.staging_schema)
    with sync_engine.connect() as connection:
        # create abstract orm object based on the existing schema
        table = Table(table_name, metadata, autoload_with=connection)

        with sync_session_factory() as session:
            try:
                # convert dataframe into json
                df['id'] = df.apply(row_to_uuid, axis=1)  # create a uuid based on the data in the row
                records = df.to_dict(orient='records')

                # Create insert statement
                stmt = insert(table).on_conflict_do_nothing(index_elements=['id'])

                # Execute the insert statement with the records
                connection.execute(stmt, records)
                session.commit()
                session.close()

            except Exception as e:
                logging.exception("Error while inserting records: ", exc_info=e)
                session.rollback()

        connection.commit()
        connection.close()


async def populate_dimensions_on_startup(db: AsyncSession) -> None:
    """
    Used to start the database with any base dimension data
    :param db:
    :return:
    """
    logging.info('Init database dimensions...')


class SchemaInit:
    @staticmethod
    async def populate_default_data(
            db: AsyncSession, model_class: Type[Base], default_data: List[Base]
    ) -> None:
        """
        Function to check if dimensions already exist, and to use the target SQL model to insert data
        :param db:
        :param model_class:
        :param default_data:
        :return:
        """

        # Check if the table associated with the model_class is empty
        result = await db.execute(select(model_class))
        existing_data = result.scalars().all()

        if not existing_data:
            db.add_all(default_data)
            await db.commit()
            await db.aclose()

    @staticmethod
    async def create_schema(db: AsyncSession, schema_name: str) -> None:
        await db.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name.strip().replace(' ', '_').lower()}"))
        await db.commit()
