from xbbg import blp
import pandas as pd
import os
import time
import sys

data = pd.read_excel('long_universe.xlsx')

print(data)

folder_dict = {
    'PX_OPEN': 'equity',
    'RETURN_COM_EQY': 'roe',
    'BEST_PE_RATIO': 'fwd_pe',
    'BEST_EPS': 'eps',
    'BEST_DIV_YLD': 'div_yield',
    'CURRENT_EV_TO_T12M_EBIT': 'ev_ebit',
    'RSI_30D': 'rsi_30d',
}

tickers = data['Ticker'].values.tolist()
fields = ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "PX_VOLUME"]
#fields = ["RSI_30D"]

for ticker in tickers:
    start_date = '2020-01-01'
    end_date = "2024-04-08"
    data = blp.bdh(ticker, fields, start_date=start_date, end_date=end_date, Days="A")
    print(data)

    ticker_name = ticker.split()[0]
    ticker_country = ticker.split()[1]

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

    time.sleep(0.05)