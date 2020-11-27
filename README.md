# This is fast binance ohlcv history downloader with asyncio
This Repository allows to super fast download historical ohlcv data from binance.
The data is stored inside a hdf5 file and can be retrieved via the `hdf_to_df` function or you can convert the data into seperate  csv files 

### How to use?
1. install reqiermetns `pip install -r requirements.txt` 
2. edit parameters inside `main.py` to your likings 
3. simply run `python main.py`

### Example use
**first set parameteres**
`file_path = ''`
`base = 'USDT'`
`timeframe = '1h'`

**Download OHLCV into hdf file**
`get_data(base=base, quotes='ETH', timeframe=timeframe, file_path=file_path)`

**get df from hdf file**
`df = df_from_hdf(file_path='binance_ohlcv.hdf5', symbol='ETH', timeframe=timeframe)`

**Convert hdf file into separate csv files one file per symbol**
`hdf_to_csv(symbols=['ETH'], timeframe=timeframe, hdf_file_path='binance_ohlcv.hdf5', csv_file_path=file_path)`

### Note
you can run all functions at once or each functions individually 
