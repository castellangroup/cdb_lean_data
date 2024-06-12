from xbbg import blp
import pandas as pd
import logging, coloredlogs, sys
import time
import gen_ticker_list

logging.basicConfig()
logger = logging.getLogger(name='Rolling EQS')
coloredlogs.install(logger=logger)
logger.propagate = False

coloredFormatter = coloredlogs.ColoredFormatter(
    fmt='[%(name)s] %(asctime)s %(funcName)s %(lineno)-3d  %(message)s',
    level_styles=dict(
        debug=dict(color='white'),
        info=dict(color='blue'),
        warning=dict(color='yellow', bright=True),
        error=dict(color='red', bold=True, bright=True),
        critical=dict(color='black', bold=True, background='red'),
    ),
    field_styles=dict(
        name=dict(color='white'),
        asctime=dict(color='white'),
        funcName=dict(color='white'),
        lineno=dict(color='white'),
    )
)

ch = logging.StreamHandler(stream=sys.stdout)
ch.setFormatter(fmt=coloredFormatter)
logger.addHandler(hdlr=ch)
logger.setLevel(level=logging.DEBUG)

#blp.bdp('MSFT US Equity', 'PX_LAST')
#print(blp.beqs('intl equity screen', '2024-06-03'))

def main():
    df = pd.DataFrame()
    for quarter_date in gen_quarter_dates_list(2020, 2023):
        df = get_screen_data('intl equity screen', quarter_date, df)
    logger.info(f"Data:\n{df}")
    df.to_csv('rolling intl_equity_screen.csv', index=False)
    gen_ticker_list.create_ticker_list(df)

def get_screen_data(screen_name, date, df):
    logger.info(f'Getting screen data for {date}')
    try:
        data = blp.beqs(screen_name, date)
        wait_time = 5
        while data.empty:
            logger.warning(f'No data found for {date}. Waiting {wait_time} seconds...')
            time.sleep(wait_time)
            data = blp.beqs(screen_name, date)
            wait_time += 5
        data = data.reset_index()
        df[date] = data['ticker']
        logger.info(f"Found {len(data)} tickers for {date}")
    except KeyError as e:
        logger.error(f'No data found for {date}\ndata: {data}\nerror: {e}')
    return df

def gen_quarter_dates_list(year_start, year_end):
    quarter_dates = []
    for year in range(year_start, year_end+1):
        quarter_dates.append(f'{year}-01-01')
        quarter_dates.append(f'{year}-04-01')
        quarter_dates.append(f'{year}-07-01')
        quarter_dates.append(f'{year}-10-01')
    return quarter_dates



if __name__ == '__main__':
    main()