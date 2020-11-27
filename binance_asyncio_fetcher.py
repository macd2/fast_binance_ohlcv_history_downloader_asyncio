last_dates = {}
import asyncio

import ccxt.async_support as ccxt
import pandas as pd

last_dates = {}


def get_data(base, quotes, timeframe, file_path):
    async def fetch_ticker(exchange, base, quote, timeframe, limit, store):

        global last_dates
        symbol = quote + "/" + base
        table = "ohlcv" + "/" + exchange.id + "/" + symbol + "/t" + timeframe

        start_date = last_dates[base][quote]

        if (start_date == -1):
            print(f"Fully downloaded {symbol} ...")
            return

        # Fetch the data
        def to_readable(timestamp):
            return pd.to_datetime(timestamp, origin='unix', unit='ms')

        print(f"Downloading {symbol} for timeframe {timeframe} from {to_readable(start_date)}")

        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit, since=start_date + 1)

        # print(ohlcv)
        if len(ohlcv) <= 1:
            print(f"Fully downloaded {symbol} ...")
            last_dates[base][quote] = -1
            return

        # Remove any overlapping data
        first_date = int(ohlcv[0][0])
        last_date = int(ohlcv[-2][0])

        if (last_date == start_date):
            print(f"Fully downloaded {symbol} ...")
            last_dates[base][quote] = -1
        else:
            last_dates[base][quote] = last_date
            print(f"Downloaded {symbol} for timeframe {timeframe} from {to_readable(first_date)} to {to_readable(last_date)}")
            # Store everything expect the last row ...
            df = pd.DataFrame.from_records(ohlcv[:-1], columns=['date', 'o', 'h', 'l', 'c', 'v'])
            store.append(table, df, data_columns=True, format='t', index=False)

    async def fetch_symbols(exchange):
        await exchange.load_markets()
        return exchange.symbols

    async def fetch_tickers(exchange, timeframe, base, quotes, store, limit):
        # await exchange.load_markets()
        exchange.load_markets()
        input_coroutines = [fetch_ticker(exchange, base, quote, timeframe, limit, store) for quote in quotes]
        await asyncio.gather(*input_coroutines, return_exceptions=False)

    def get_symbols(exchange, base=None):
        symbols = asyncio.get_event_loop().run_until_complete(fetch_symbols(exchange))
        if base != None:
            quotes = [x.replace("/" + base, "") for x in exchange.symbols if "/" + base in x]
        else:
            quotes = symbols
        return quotes

    def merge_data(base, exchange, quotes, f):
        data = []
        with pd.HDFStore(f) as db:
            for q in quotes:
                df = db.select("ohlcv/" + exchange.id + "/" + q + "/" + base)
                df.drop(columns=['h', 'l', 'v', 'o'], inplace=True)
                df.rename(inplace=True, columns={'c': q})
                data.append(df)
            final = data[0]
            for j in range(1, len(quotes)):
                final = final.merge(data[j], on='date')
        return final

    def download(f, exchange, timeframe, base, quotes, limit=1000):
        with pd.HDFStore(f) as store:
            asyncio.get_event_loop().run_until_complete(fetch_tickers(exchange, timeframe, base, quotes, store, limit))

    exchange_class = getattr(ccxt, 'binance')
    exchange = exchange_class({
        'timeout': 30000,
        'enableRateLimit': True,
    })

    f = file_path + 'binance' + '_ohlcv' + '.hdf5'
    last_dates[base] = {}

    if (quotes == None):
        # Fetch all quotes from the server
        quotes = get_symbols(exchange, base)

    # Fetch all quotes from the server
    with pd.HDFStore(f) as store:
        for quote in quotes:
            last_dates[base][quote] = 0

            # Update using the last know timestamp from the HDF5 file ...
            table = "ohlcv" + "/" + exchange.id + "/" + quote + "/" + base + "/t" + timeframe
            try:
                s = store.select(table)
                last = sorted(s.date.unique())[-1]
                last_dates[base][quote] = last + 1
            except Exception as e:
                print(e)
                last_dates[base][quote] = 0

    remaining = [k for (k, v) in last_dates[base].items() if v != -1]

    while len(remaining) != 0:
        download(f, exchange, timeframe, base, remaining)
        remaining = [k for (k, v) in last_dates[base].items() if v != -1]

    asyncio.get_event_loop().run_until_complete(exchange.close())


def update_datasets(hdf_file):
    """Update dataset across all groups in HDF5 file."""
    import h5py

    def h5py_dataset_iterator(g, prefix=''):
        for key in g.keys():
            item = g[key]
            path = '{}/{}'.format(prefix, key)
            if isinstance(item, h5py.Dataset):  # test for dataset
                yield (path, item)
            elif isinstance(item, h5py.Group):  # test for group (go down)
                yield from h5py_dataset_iterator(item, path)

    with h5py.File(hdf_file, 'r') as f:
        for (path, dset) in h5py_dataset_iterator(f):
            print(path, dset)
    return None


# df from hadf file
def df_from_hdf(file_path, timeframe, symbol=None):
    path = '/ohlcv/binance/' + symbol + '/USDT/t' + timeframe + '/'
    df = pd.read_hdf(file_path, key=path, mode='r')

    # Rename Columnes
    names = ['time', 'open', 'high', 'low', 'close', 'volume']  # read in specific file
    df.columns = names


def hdf_to_csv(symbols: list, timeframe, hdf_file_path, csv_file_path):
    update_datasets(hdf_file_path)

    for i in symbols:
        path = '/ohlcv/binance/' + i + '/USDT/t' + timeframe + '/'  # internal hdf path
        with pd.HDFStore(hdf_file_path) as db:
            data = db.select(path)

        # Reorder the columes
        new_order = [0, 3, 2, 1, 4, 5]
        df = data[data.columns[new_order]]

        # Rename Columnes
        names = ['time', 'low', 'high', 'open', 'close', 'volume']  # read in specific file
        df.columns = names
        p = csv_file_path + '/' if csv_file_path else ''
        df.to_csv(p + f'{timeframe}_{i}_binance_ohlcv.csv', header=True, index=False)
        print(f'Saved: {i}-USD_{timeframe}.csv')
        print(f'Date Range is: ',
              pd.to_datetime(df['time'], unit='ms').dt.strftime("%d-%m-%Y, %H:%M:%S").values[0],
              'to',
              pd.to_datetime(df['time'], unit='ms').dt.strftime("%d-%m-%Y, %H:%M:%S").values[-1]
              )
