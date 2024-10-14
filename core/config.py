import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# Load the stored environment variables
load_dotenv()


class Settings(BaseSettings):
    app_name: str = str(os.environ.get('app_name'))
    debug_logs: bool = bool(os.environ.get('debug_logs'))
    dev: bool = bool(os.environ.get('dev'))
    init_db: bool = bool(os.environ.get('init_db'))
    db_string: str = str(os.environ.get('db_string'))
    db_string_async: str = str(os.environ.get('db_string_async'))
    echo_sql: bool = True
    test: bool = False
    staging_schema: str = str(os.environ.get('staging_schema'))
    dw_schema: str = str(os.environ.get('dw_schema'))


class CelerySettings(BaseSettings):
    celery_app_name: str = str(os.environ.get('app_name'))
    celery_broker: str = str(os.environ.get('celery_broker'))
    celery_backend: str = str(os.environ.get('celery_backend'))

class CryptoAPIs(BaseSettings):
    coingecko_api_key: str = str(os.environ.get('coingecko_api_key'))
    coingecko_api_url: str = str(os.environ.get('coingecko_api_url'))
    dune_analytics_key: str = str(os.environ.get('dune_analytics_key'))
    dune_analytics_url: str = str(os.environ.get('dune_analytics_url'))


settings = Settings()
cyrpto_api_settings = CryptoAPIs()
celery_settings = CelerySettings()
