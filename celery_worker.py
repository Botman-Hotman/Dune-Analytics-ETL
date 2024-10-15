from celery import Celery
from core.config import celery_settings
from celery.schedules import crontab
from datetime import datetime, timedelta

celery_app = Celery(
    main=celery_settings.celery_app_name,
    broker=celery_settings.celery_broker,
    backend=celery_settings.celery_backend
    , include=
    [
        'services.import_dune_data_to_staging'
        , 'services.import_coingecko_to_staging'
    ]
)
celery_app.conf.broker_connection_retry_on_startup = True

celery_app.conf.beat_schedule = {
    "update-dune-data-task": {
        "task": 'services.import_dune_data_to_staging.update_dune_data',
        "schedule": crontab(hour=9, minute=0)
    }

    , "update-coingecko-data-ethereum-task": {
        "task":  'services.import_coingecko_to_staging.update_coingecko_prices',
        "schedule": crontab(hour=9, minute=0),
        "args": ('ethereum',)
    }
    , "update-coingecko-data-usdc-task": {
        "task":  'services.import_coingecko_to_staging.update_coingecko_prices',
        "schedule": crontab(hour=9, minute=0),
        "args": ('usd-coin',)
    }
}