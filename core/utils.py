import pandas as pd
import logging



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


async def df_to_postgres_schema(df: pd.DataFrame, table_name: str) -> None:
    """
    Check if a table exists in the database, if not create it and its primary key based on dataframe schema
    :param table_name:
    :param df:
    :return:
    """
    # map pandas data column types into postgres
    df.columns = [f"column_{i}" if col.startswith('Unnamed') else col.strip().replace(' ', '_').lower() for i, col in enumerate(df.columns)]
    column_definitions: list = [f""""{col}" {map_dtype_to_postgres(df[col].dtype)}""" for col in df.columns]

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
            # Create table if it doesn't exist, or ensure structure if it does
            create_table_query = f"""CREATE TABLE IF NOT EXISTS {settings.staging_schema}."{table_name}" ({", ".join(column_definitions)});"""
            await conn.execute(text(create_table_query))
            await conn.commit()


def df_to_abstract_orm(df: pd.DataFrame, table_name: str) -> None:
    """
    Convert pandas dataframe into orm object to insert into a database through sql alchemy
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
                records = df.to_dict(orient='records')

                # Create insert statement
                stmt = insert(table)

                # Execute the insert statement with the records
                connection.execute(stmt, records)
                session.commit()
                session.close()

            except Exception as e:
                logging.exception(e)
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