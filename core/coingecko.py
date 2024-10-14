import requests
import logging
from core.config import cyrpto_api_settings
from core.utils import convert_to_unix_timestamp


def authentication() -> None | bool:
    url_string = f"{cyrpto_api_settings.coingecko_api_url}/ping?x_cg_demo_api_key={cyrpto_api_settings.coingecko_api_key}"

    response = requests.get(url_string)

    if 200 <= response.status_code <= 210:
        logging.debug(response.text)
        return True

    else:
        logging.warning(f"{response.content} -> {response.status_code}")
        return


def get_historical_price(coin_id, datetime_from: str, datetime_to: str, vs_currency: str = 'usd'):
    """

    :param coin_id: bitcoin, ethereum, usd-coin
    :param datetime_from: YYYY-MM-DD HH:MM:SS
    :param datetime_to: YYYY-MM-DD HH:MM:SS
    :param vs_currency: usd, btc, xrp, eur, gbp ect
    :return:
    """
    url = f"{cyrpto_api_settings.coingecko_api_url}/coins/{coin_id}/market_chart/range"

    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": cyrpto_api_settings.coingecko_api_key
    }

    params = {
        "vs_currency": vs_currency
        , "from": convert_to_unix_timestamp(datetime_from)
        , "to": convert_to_unix_timestamp(datetime_to)
    }
    response = requests.get(url, params=params, headers=headers)

    if 200 <= response.status_code <= 210:
        data = response.json()
        if 'prices' in data:
            return data

        return

    else:
        logging.warning(f"{response.content} -> {response.status_code}")
        return


def get_coin_list():
    url = f"{cyrpto_api_settings.coingecko_api_url}/coins/list"

    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": cyrpto_api_settings.coingecko_api_key
    }

    response = requests.get(url, headers=headers)

    if 200 <= response.status_code <= 210:
        data = response.json()
        return data

    else:
        logging.warning(f"{response.content} -> {response.status_code}")
        return


if __name__ == '__main__':
    a = get_historical_price('ethereum', '2024-10-12 00:00:00', '2024-10-12 06:00:00')
    b = get_historical_price('usd-coin', '2024-10-12 00:00:00', '2024-10-12 06:00:00')
    c = 1
