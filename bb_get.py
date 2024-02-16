from xbbg import blp
import pandas as pd
import os
import time
import sys

data = pd.read_excel('grid_doc.xlsx')

print(data)

tickers = data['Ticker'].values.tolist()
#fields = ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "PX_VOLUME"]
fields = ["BEST_EPS", "BEST_EPS_NXT_YR"]

for ticker in tickers:
    start_date = '2010-01-01'
    end_date = "2024-02-15"
    data = blp.bdh(ticker, fields, start_date=start_date, end_date=end_date, Days="A")
    print(data)

    ticker_name = ticker.split()[0]
    ticker_country = ticker.split()[1]

    if not os.path.exists(f'factor/eps/{ticker_country}'):
        os.makedirs(f'factor/eps/{ticker_country}')

    data.to_csv(f'factor/eps/{ticker_country}/{ticker_name}.csv', index=True, header=True)
    print(f'{ticker_name} data saved in factor/eps/{ticker_country}/{ticker_name}.csv')

    time.sleep(0.05)