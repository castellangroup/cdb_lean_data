import pandas as pd
import logging, coloredlogs, sys
import os

logging.basicConfig()
logger = logging.getLogger(name='gen_ticker_list')
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


def create_ticker_list(df, type='Equity'):
    ticker_list = df.stack().reset_index(drop=True)
    # remove duplicates
    ticker_list = ticker_list.drop_duplicates()
    # rename the first row to 'ticker'
    ticker_list = ticker_list.rename('tickers')

    # loop through each row and add ' Index' to the end of each ticker
    for index, ticker in ticker_list.items():
        ticker_list[index] = ticker + ' ' + type

    file_name = type + '_rolling_eqs_ticker_list.csv'
    output_path = os.path.join(os.getcwd(), '..', 'ticker_lists', file_name)
    ticker_list.to_csv(output_path, index=False)
    logger.info(f"{type} Ticker list saved to ticker_list.csv")

if __name__ == '__main__':
    df = pd.read_csv('rolling intl_equity_screen.csv')
    create_ticker_list(df)