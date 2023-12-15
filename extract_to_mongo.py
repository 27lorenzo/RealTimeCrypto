import pandas as pd
import requests
import config
from pymongo import MongoClient
import logging

client = None
database = "projects"
collection = "CoinMarketCap"


def get_crypto_cata():
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    parameters = {
        'start': '1',
        'limit': '1000',
        'convert': 'USD'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key
    }

    try:
        response = requests.get(url, headers=headers, params=parameters)
        response.raise_for_status()

        data = response.json()
        cryptocurrencies = data['data']

        crypto_list = []
        for crypto in cryptocurrencies:
            info = {
                'Name': crypto['name'],
                'Symbol': crypto['symbol'],
                'Rank': crypto['cmc_rank'],
                'Price': crypto['quote']['USD']['price'],
                'Volume24h': crypto['quote']['USD']['volume_24h'],
                'MarketCap': crypto['quote']['USD']['market_cap'],
                'TotalSupply': crypto['total_supply'],
                'CirculatingSupply': crypto['circulating_supply'],
                'MaxSupply': crypto['max_supply'],
                'PercentChange1h': crypto['quote']['USD']['percent_change_1h'],
                'PercentChange24h': crypto['quote']['USD']['percent_change_24h']
            }
            crypto_list.append(info)
        df = pd.DataFrame(crypto_list)
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error in the request: {e}")


def create_client(mongo_server):
    global client
    if not client:
        client = MongoClient(host=[mongo_server])


def log(verbose, s):
    if verbose:
        print(s)


def load_data(df):
    try:
        db = client[database]
        col = db[collection]
        data = df.to_dict(orient='records')
        col.insert_many(data)
        logging.info(f"Data inserted to: {database}/{collection}")

    except Exception as e:
        logging.error(f"Error loading data: {e}")


def main():
    df = get_crypto_cata()
    print(df)
    try:
        create_client(mongo_server)
        load_data(df)
    finally:
        if client:
            client.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    c = config.config()
    api_key = c.readh('coinmarketcap_key', 'api_key')
    mongo_server = c.readh('mongodb', 'server') or 'localhost'
    main()
