import os
import glob
import pandas as pd
import logging, coloredlogs
import sys

data_folders = ['equity', 'factor', 'fx']
output_folder = 'outputs/'
ticker_list_output = 'ticker_lists/other/'
logging.basicConfig()
logger = logging.getLogger(name='BulkFileGen')
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


def main():
    #convert_currency()
    #for data_folder in data_folders:
        #create_combined_data(data_folder, data_folder)
    create_combined_data('equity', 'equity', False)
    create_combined_data('factor\eps', 'eps')

def convert_currency():
    """
    Testing version of convert_currency(), use the other version in create_combined_data() instead\n
    """
    directories = [d for d in os.listdir('equity') if os.path.isdir(os.path.join('equity', d))]
    for directory in directories:
        country = directory

        try:
            country_currency = pd.read_csv('fx/country_currency.csv')
            country_currency = country_currency[country_currency['Country'] == country]
            if (country == 'NA'):
                print("HERE")
                print(country_currency)
            currency = country_currency['Currency'].values[0]
        except IndexError:
            print(f"Error: Country {country} not found in country_currency.csv")
            currency = 'USD'
        
        if currency == 'USD':
            continue
        

        currency_rate = pd.read_csv('fx/rates/USD_' + currency + '.csv')
        csv_files = glob.glob(os.path.join('equity', directory, '*.csv'))
        for csv_file in csv_files:
            # make the second row be the columns when reading the csv
            df = pd.read_csv(csv_file, skiprows=1)
            print(f"Stock: {csv_file}  Currency: {currency}  Country: {country}")

            currency_rate = format_currency_rate(currency_rate)

            # rename the first column of df to be 'Date'
            df.rename(columns={df.columns[0]: 'Date'}, inplace=True)

            print(f"df: {df.tail()}")
            # multiply all columns by the currency rate for the given date
            for index, row in df.iterrows():
                date = row['Date']
                rate = currency_rate[currency_rate['Date'] == date]['Rate'].values
                if len(rate) == 0:
                    print(f"Error: No rate found for {date}")
                    continue
                rate = rate[0]
                for column in df.columns[1:]:
                    df.loc[index, column] = row[column] / rate
            print(f"df: {df.tail()}")

def convert_currency(df, directory, csv_file):
    """
    Convert the currency of the given dataframe to USD\n
    Takes the directory and CSV File \n
    Returns updated dataframe
    """
    logger.info(f"Stock: {csv_file}  Country: {directory}")
    country = directory
    try:
        country_currency = pd.read_csv('fx/country_currency.csv')
        country_currency = country_currency[country_currency['Country'] == country]
        if (country == 'NA'):
            logger.warning(f"TODO: {country_currency}")    
        currency = country_currency['Currency'].values[0]
    except IndexError:
        logger.warning(f"Country {country} not found in country_currency.csv")
        currency = 'USD'
    if currency == 'USD':
        return
    
    currency_rate = pd.read_csv('fx/rates/USD_' + currency + '.csv')
    currency_rate = format_currency_rate(currency_rate)

    equity_df = pd.read_csv(csv_file, skiprows=1)
    equity_df.rename(columns={equity_df.columns[0]: 'Date'}, inplace=True)

    logger.debug(f"Equity Dataframe:\n {equity_df.tail()}")
    equity_df = replace_currency_rate(equity_df, currency_rate)
    logger.debug(f"Equity Dataframe:\n {equity_df.tail()}")

    return equity_df

def format_currency_rate(currency_df):
    """
    Helper function for convert_currency()\n
    Rename the columns of the currency rate dataframe to 'Date' and 'Rate'
    Convert the 'Rate' column to a numeric type
    Remove the first row of the dataframe\n
    Returns Updated Dataframe
    """
    currency_df.rename(columns={currency_df.columns[0]: 'Date', currency_df.columns[1]: 'Rate'}, inplace=True)            
    currency_df = currency_df.iloc[1:]
    currency_df.reset_index(drop=True, inplace=True)
    currency_df.loc[:, 'Rate'] = pd.to_numeric(currency_df['Rate'], errors='coerce')

    return currency_df
def replace_currency_rate(equity_df, currency_rate):
    """
    Helper function for convert_currency()\n
    Replace the values in the equity dataframe with the currency rate\n
    Returns updated dataframe
    """
    for index, row in equity_df.iterrows():
        date = row['Date']
        rate = currency_rate[currency_rate['Date'] == date]['Rate'].values
        if len(rate) == 0:
            logger.warning(f"No rate found for {date}")
            continue
        rate = rate[0]
        for column in equity_df.columns[1:]:
            if column == 'Date' or column == 'PX_VOLUME':
                continue
            equity_df.loc[index, column] = row[column] / rate
    return equity_df

def create_combined_data(folder, type='equity', convert_to_USD=False):
    """
    Combine all csv files in the given folder into a single dataframe\n
    Renames the columns of the dataframe
    Converts the currency of the dataframe if convert_to_USD is True\n
    Writes the combined dataframe to a csv file
    """
    combined_df = pd.DataFrame()
    
    # get the number of files in folder
    num_files = len(glob.glob(os.path.join(folder, '**/*.csv'), recursive=True))
    currFileNum = 0

    directories = [d for d in os.listdir(folder) if os.path.isdir(os.path.join(folder, d))]
    for directory in directories:
        csv_files = glob.glob(os.path.join(folder, directory, '*.csv'))
        for csv_file in csv_files:
            currFileNum += 1
            logger.info(f"Processing {csv_file} ({currFileNum}/{num_files})")
            if('RIO' in csv_file and 'LN' in csv_file):
                logger.info("Skipping RIO in LN")
                continue
            df = pd.read_csv(csv_file)

            df = rename_columns(df, type)

            if type=='equity' and convert_to_USD:
                df = convert_currency(df, directory, csv_file)

            try:
                combined_df = pd.merge(combined_df, df, how='outer')
            except pd.errors.MergeError:
                logger.error("Error merging dataframes")
                combined_df = df

    combined_df = combined_df[combined_df.iloc[:, 0] >= '2020-01-01']
    try:
        combined_df.to_csv(output_folder + 'combined_' + type + '_data.csv', index=False)
    except PermissionError:
        logger.error("Error writing to file. Please close the file and try again.")
        return
    combined_df.iloc[:, 0] = pd.to_datetime(combined_df.iloc[:, 0])

    for directory in directories:
        csv_files = glob.glob(os.path.join(folder, directory, '*.csv'))
        for csv_file in csv_files:
            df = pd.read_csv(csv_file)
            df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])        
            df.iloc[:, 0] = df.iloc[:, 0].isin(combined_df.iloc[:, 0])
            if False in df.iloc[:, 0]:
                print(csv_file)

    column_names = list(combined_df.columns)
    column_index = list(range(len(column_names)))
    column_dict = dict(zip(column_names, column_index))
    column_df = pd.DataFrame(column_dict.items(), columns=['Column Name', 'Index'])
    column_df.to_csv(ticker_list_output + type + '_index.csv', index=False)

        
def rename_columns(df, type='equity'):
    """
    Helper function for create_combined_data()\n
    Renames the columns of the dataframe based on the type\n
    Returns updated dataframe
    """
    columns = list(df.columns)
    for index, column in enumerate(columns):
        split = column.split('.')
        if type=='equity':
            if len(split) == 1:
                columns[index] = column + " OPEN"
            elif split[1] == '1':
                columns[index] = split[0] + " HIGH"
            elif split[1] == '2':
                columns[index] = split[0] + " LOW"
            elif split[1] == '3':
                columns[index] = split[0] + " LAST"
            elif split[1] == '4':
                columns[index] = split[0] + " VOLUME"
        elif type=='eps':
            if len(split) == 1:
                columns[index] = column + " BEST_EPS"
            elif split[1] == '1':
                columns[index] = split[0] + " BEST_EPS_NXT_YR"
    df.columns = columns
    return df

if __name__ == "__main__":
    main()