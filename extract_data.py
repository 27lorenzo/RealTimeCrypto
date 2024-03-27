from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.streaming import StreamingContext
import time
import requests
import json
from pyspark.sql.functions import current_timestamp, current_date, date_format

spark = SparkSession.builder \
    .appName("RealTimeCrypto") \
    .master("local[2]") \
    .getOrCreate()

spark_s = StreamingContext(spark.sparkContext, 60)

schema = StructType([
    StructField('Name', StringType(), False),
    StructField('Symbol', StringType(), False),
    StructField('Rank', IntegerType(), False),
    StructField('Price', FloatType(), False),
    StructField('Volume24h', FloatType(), False),
    StructField('MarketCap', FloatType(), False),
    StructField('TotalSupply', FloatType(), False),
    StructField('CirculatingSupply', FloatType(), False),
    # StructField('MaxSupply', FloatType(), True),
    StructField('PercentChange1h', FloatType(), False),
    StructField('PercentChange24h', FloatType(), False)
])

schema_prices = StructType([
    StructField('Coin', StringType(), False),
    StructField('Price', FloatType(), False),
    StructField('Date', DateType(), False),
    StructField('Time', DateType(), False)
])


def get_crypto_data():
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    parameters = {
        'start': '1',
        'limit': '1000',
        'convert': 'USD'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': 'c07abb26-af8b-4216-9ee6-eed73ca0bd2a'
    }

    try:
        response = requests.get(url, headers=headers, params=parameters)
        response.raise_for_status()

        data = response.json()
        file_path = "output/response.json"
        with open(file_path, "w") as file:
            json.dump(data, file)

    except requests.exceptions.RequestException as e:
        print(f"Error in the request: {e}")


def read_json_file():
    file_path = "output/response.json"
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


def process_data(df):
    cryptocurrencies = df['data']

    crypto_list = []
    for crypto in cryptocurrencies:
        info = {
            'Name': crypto['name'],
            'Symbol': crypto['symbol'],
            'Rank': crypto['cmc_rank'],
            'Price': float(crypto['quote']['USD']['price']),
            'Volume24h': float(crypto['quote']['USD']['volume_24h']),
            'MarketCap': float(crypto['quote']['USD']['market_cap']),
            'TotalSupply': float(crypto['total_supply']),
            'CirculatingSupply': float(crypto['circulating_supply']),
            # 'MaxSupply': float(str(crypto['max_supply'])),
            'PercentChange1h': float(crypto['quote']['USD']['percent_change_1h']),
            'PercentChange24h': float(crypto['quote']['USD']['percent_change_24h'])
        }
        crypto_list.append(info)
    df = spark.createDataFrame(crypto_list, schema=schema)
    return df


def extract_coins(df, coin):
    coin_data = df.filter(df.Symbol == coin)
    return coin_data


def compare_coin_price(new_coin_data, prices_df):
    new_prices_df = new_coin_data.select('Symbol', 'Price')\
                        .withColumn('Date', current_date()) \
                        .withColumn('Time', date_format(current_timestamp(), 'HH:mm:ss'))
    prices_df = prices_df.union(new_prices_df)
    return prices_df


if __name__ == '__main__':
    prices_df = spark.createDataFrame([], schema_prices)
    while True:
        get_crypto_data()
        data = read_json_file()
        df = process_data(data)
        coin_data = extract_coins(df, 'SOL')
        prices_df = compare_coin_price(coin_data, prices_df)
        prices_df.show()
        time.sleep(60)
