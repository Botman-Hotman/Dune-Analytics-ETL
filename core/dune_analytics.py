
from dune_client.client import DuneClient
from core.config import cyrpto_api_settings

dune = DuneClient(
    api_key=cyrpto_api_settings.dune_analytics_key,
    base_url=cyrpto_api_settings.dune_analytics_url,
    request_timeout=300  # request will time out after 300 seconds
)