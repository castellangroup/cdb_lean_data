from xbbg import blp
import pandas as pd
import os
import time
import sys

#data = pd.read_excel('long_universe.xlsx')
#data = pd.read_csv('ticker_lists/ti_hist_ticks.csv')
#data = pd.read_excel('ticker_lists/etf_and_index.xlsx')
data = pd.read_csv('ticker_lists/Equity_rolling_eqs_ticker_list.csv')

folder_dict = {
    'PX_OPEN': 'equity',
    'RETURN_COM_EQY': 'roe',
    'BEST_PE_RATIO': 'fwd_pe',
    'BEST_EPS': 'eps',
    'BEST_DIV_YLD': 'div_yield',
    'CURRENT_EV_TO_T12M_EBIT': 'ev_ebit',
    'RSI_30D': 'rsi_30d',
    'BEST_SALES': 'revenue',
}

tickers = data['tickers'].values.tolist()
#tickers = ['SPDAUDT Index']
# fields = ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "PX_VOLUME"]
fields = ["BEST_EPS"]

# output_dir = 'rolling_eqs/Equity'
output_dir = 'rolling_eqs/factor/eps'
os.makedirs(output_dir, exist_ok=True)

i = 0
for ticker in tickers:

    i += 1
    print(f"Getting data for {ticker} ({i}/{len(tickers)})")

    start_date = '1980-01-01'
    end_date = "2024-05-29"
    data = blp.bdh(ticker, fields, start_date=start_date, end_date=end_date, Days="A")

    processed_ticker = ticker[:-6]

    country_output_dir = os.path.join(output_dir, processed_ticker.split()[1])
    os.makedirs(country_output_dir, exist_ok=True)
    csv_file_path = os.path.join(country_output_dir, f'{processed_ticker}.csv')
    ticker_df = pd.DataFrame(data)
    
    try:
        ticker_df.to_csv(csv_file_path)
    except OSError as e:
        print(f'Error: Unable to save {processed_ticker} to path {csv_file_path}: {e}')
        continue

    '''
    ticker_name = ticker.split()[0]
    ticker_country = ticker.split()[1]

    ticker_name = ticker_name.replace('/', '.')
    
    #path based on folder_dict
    field = fields[0]
    folder = folder_dict[field]

    if folder == 'equity':
        if not os.path.exists(f'equity/{ticker_country}'):
            os.makedirs(f'equity/{ticker_country}')

        data.to_csv(f'equity/{ticker_country}/{ticker_name}.csv', index=True, header=True)
        print('Data saved to ' + f'equity/{ticker_country}/{ticker_name}.csv')

    else:
        if not os.path.exists(f'factor/{folder}/{ticker_country}'):
            os.makedirs(f'factor/{folder}/{ticker_country}')

        data.to_csv(f'factor/{folder}/{ticker_country}/{ticker_name}.csv', index=True, header=True)
        print('Data saved to ' + f'factor/{folder}/{ticker_country}/{ticker_name}.csv')

    '''
    time.sleep(0.05) 