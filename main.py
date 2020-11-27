import requests
from binance_asyncio_fetcher import get_data, df_from_hdf, hdf_to_csv

# Get all symbols from binance
re = requests.get('http://api.binance.com/api/v3/exchangeInfo').json()
quotes = [x['symbol'].replace('USDT', '/USDT').split('/')[0] for x in re['symbols'] if x['status'] == 'TRADING' and x['quoteAsset'] == 'USDT' and x['isSpotTradingAllowed'] == True]

# Define your symbols manually
# quotes = ['BTC','XRP','ETH','LTC','MATIC','EOS','ATOM','ADA','XLM']

# Set Parameters
file_path = ''
base = 'USDT'
timeframe = '1h'

# Download OHLCV into hdf file
get_data(base=base, quotes=quotes[-2:], timeframe=timeframe, file_path=file_path)

# get df from hdf file
df = df_from_hdf(file_path='binance_ohlcv.hdf5', symbol=quotes[-2], timeframe=timeframe)

# Convert hdf file into separate csv files one file per symbol
hdf_to_csv(symbols=quotes[-2:], timeframe=timeframe, hdf_file_path='binance_ohlcv.hdf5', csv_file_path=file_path)
