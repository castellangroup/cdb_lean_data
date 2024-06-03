import os
import pandas as pd
import sqlite3

def load_equity_to_sql(db_path, equity_folder):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Loop through the country code folders in the equity folder
        for country_code in os.listdir(equity_folder):
            country_path = os.path.join(equity_folder, country_code)

            if os.path.isdir(country_path):
                # Loop through the CSV files in the country folder
                for stock_file in os.listdir(country_path):
                    stock_path = os.path.join(country_path, stock_file)

                    if stock_file.endswith('.csv'):
                        ticker = os.path.splitext(stock_file)[0]

                        # Read the CSV file into a DataFrame
                        try:
                            df = pd.read_csv(stock_path, skiprows=2)

                            # Rename columns
                            df.columns = ['date', 'open', 'high', 'low', 'last', 'volume']

                            # Add ticker and country columns
                            df['ticker'] = ticker
                            df['country'] = country_code
                            df['ticker_country'] = df['ticker'] + '_' + df['country']

                            # Reorder columns to match the SQL table
                            df = df[['ticker_country', 'ticker', 'country', 'date', 'open', 'high', 'low', 'last', 'volume']]

                            # Insert data into SQL table
                            df.to_sql('prices', conn, if_exists='append', index=False)
                        except Exception as e:
                            pass

    except Exception as e:
        pass

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("Prices table data loaded successfully!")

def load_div_yield_to_sql(db_path, factor_folder):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    div_yield_folder = os.path.join(factor_folder, 'div_yield')

    try:
        # Loop through the country code folders in the div_yield folder
        for country_code in os.listdir(div_yield_folder):
            country_path = os.path.join(div_yield_folder, country_code)

            if os.path.isdir(country_path):
                # Loop through the CSV files in the country folder
                for stock_file in os.listdir(country_path):
                    stock_path = os.path.join(country_path, stock_file)

                    if stock_file.endswith('.csv'):
                        ticker = os.path.splitext(stock_file)[0]

                        # Read the CSV file into a DataFrame
                        try:
                            df = pd.read_csv(stock_path, skiprows=2)

                            # Rename columns
                            df.columns = ['date', 'dividend_yield']

                            # Handle cases where 'dividend_yield' might be empty or data starts further down
                            df['dividend_yield'] = pd.to_numeric(df['dividend_yield'], errors='coerce')

                            # Add ticker and country columns
                            df['ticker'] = ticker
                            df['country'] = country_code
                            df['ticker_country'] = df['ticker'] + '_' + df['country']

                            # Reorder columns to match the SQL table
                            df = df[['ticker_country', 'ticker', 'country', 'date', 'dividend_yield']]

                            # Insert data into SQL table
                            df.to_sql('div_yield', conn, if_exists='append', index=False)
                        except Exception as e:
                            pass

    except Exception as e:
        pass

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("Dividend yield table data loaded successfully!")

def load_dw_score_to_sql(db_path, factor_folder):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    dw_score_folder = os.path.join(factor_folder, 'dw_score')

    try:
        # Loop through the country code folders in the dw_score folder
        for country_code in os.listdir(dw_score_folder):
            country_path = os.path.join(dw_score_folder, country_code)

            if os.path.isdir(country_path):
                # Loop through the CSV files in the country folder
                for stock_file in os.listdir(country_path):
                    stock_path = os.path.join(country_path, stock_file)

                    if stock_file.endswith('.csv'):
                        ticker = os.path.splitext(stock_file)[0]

                        # Read the CSV file into a DataFrame
                        try:
                            df = pd.read_csv(stock_path, skiprows=1)

                            # Rename columns
                            df.columns = ['ticker', 'dw_score', 'date']

                            # Drop the redundant 'ticker' column from the DataFrame
                            df = df.drop(columns=['ticker'])

                            # Handle cases where 'dw_score' might be empty or data starts further down
                            df['dw_score'] = pd.to_numeric(df['dw_score'], errors='coerce')

                            # Add ticker and country columns
                            df['ticker'] = ticker
                            df['country'] = country_code
                            df['ticker_country'] = df['ticker'] + '_' + df['country']

                            # Reorder columns to match the SQL table
                            df = df[['ticker_country', 'ticker', 'country', 'date', 'dw_score']]

                            # Insert data into SQL table
                            df.to_sql('dw_score', conn, if_exists='append', index=False)
                        except Exception as e:
                            pass

    except Exception as e:
        pass

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("dw_score table data loaded successfully!")

def load_eps_to_sql(db_path, factor_folder):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    eps_folder = os.path.join(factor_folder, 'eps')

    try:
        # Loop through the country code folders in the div_yield folder
        for country_code in os.listdir(eps_folder):
            country_path = os.path.join(eps_folder, country_code)

            if os.path.isdir(country_path):
                # Loop through the CSV files in the country folder
                for stock_file in os.listdir(country_path):
                    stock_path = os.path.join(country_path, stock_file)

                    if stock_file.endswith('.csv'):
                        ticker = os.path.splitext(stock_file)[0]

                        # Read the CSV file into a DataFrame
                        try:
                            df = pd.read_csv(stock_path, skiprows=2)

                            # Rename columns
                            df.columns = ['date', 'eps']

                            # Handle cases where 'eps' might be empty or data starts further down
                            df['eps'] = pd.to_numeric(df['eps'], errors='coerce')

                            # Add ticker and country columns
                            df['ticker'] = ticker
                            df['country'] = country_code
                            df['ticker_country'] = df['ticker'] + '_' + df['country']

                            # Reorder columns to match the SQL table
                            df = df[['ticker_country', 'ticker', 'country', 'date', 'eps']]

                            # Insert data into SQL table
                            df.to_sql('eps', conn, if_exists='append', index=False)
                        except Exception as e:
                            pass

    except Exception as e:
        pass

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("EPS table data loaded successfully!")

def load_eps_nxt_yr_to_sql(db_path, factor_folder):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    eps_nxt_yr_folder = os.path.join(factor_folder, 'eps_nxt_yr')

    try:
        # Loop through the country code folders in the folder
        for country_code in os.listdir(eps_nxt_yr_folder):
            country_path = os.path.join(eps_nxt_yr_folder, country_code)

            if os.path.isdir(country_path):
                # Loop through the CSV files in the country folder
                for stock_file in os.listdir(country_path):
                    stock_path = os.path.join(country_path, stock_file)

                    if stock_file.endswith('.csv'):
                        ticker = os.path.splitext(stock_file)[0]

                        # Read the CSV file into a DataFrame
                        try:
                            df = pd.read_csv(stock_path, skiprows=2)

                            # Rename columns
                            df.columns = ['date', 'eps_nxt_yr']

                            # Handle cases where 'eps' might be empty or data starts further down
                            df['eps_nxt_yr'] = pd.to_numeric(df['eps_nxt_yr'], errors='coerce')

                            # Add ticker and country columns
                            df['ticker'] = ticker
                            df['country'] = country_code
                            df['ticker_country'] = df['ticker'] + '_' + df['country']

                            # Reorder columns to match the SQL table
                            df = df[['ticker_country', 'ticker', 'country', 'date', 'eps_nxt_yr']]

                            # Insert data into SQL table
                            df.to_sql('eps_nxt_yr', conn, if_exists='append', index=False)
                        except Exception as e:
                            pass

    except Exception as e:
        pass

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("Next Year EPS table data loaded successfully!")

def load_ev_ebit_to_sql(db_path, factor_folder):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    ev_ebit_folder = os.path.join(factor_folder, 'ev_ebit')

    try:
        # Loop through the country code folders in the folder
        for country_code in os.listdir(ev_ebit_folder):
            country_path = os.path.join(ev_ebit_folder, country_code)

            if os.path.isdir(country_path):
                # Loop through the CSV files in the country folder
                for stock_file in os.listdir(country_path):
                    stock_path = os.path.join(country_path, stock_file)

                    if stock_file.endswith('.csv'):
                        ticker = os.path.splitext(stock_file)[0]

                        # Read the CSV file into a DataFrame
                        try:
                            df = pd.read_csv(stock_path, skiprows=2)

                            # Rename columns
                            df.columns = ['date', 'ev_ebit']

                            # Handle cases where 'eps' might be empty or data starts further down
                            df['eps'] = pd.to_numeric(df['ev_ebit'], errors='coerce')

                            # Add ticker and country columns
                            df['ticker'] = ticker
                            df['country'] = country_code
                            df['ticker_country'] = df['ticker'] + '_' + df['country']

                            # Reorder columns to match the SQL table
                            df = df[['ticker_country', 'ticker', 'country', 'date', 'ev_ebit']]

                            # Insert data into SQL table
                            df.to_sql('ev_ebit', conn, if_exists='append', index=False)
                        except Exception as e:
                            pass

    except Exception as e:
        pass

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("ev_ebit table data loaded successfully!")

def load_fwd_pe_to_sql(db_path, factor_folder):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    fwd_pe_folder = os.path.join(factor_folder, 'fwd_pe')

    try:
        # Loop through the country code folders in the div_yield folder
        for country_code in os.listdir(fwd_pe_folder):
            country_path = os.path.join(fwd_pe_folder, country_code)

            if os.path.isdir(country_path):
                # Loop through the CSV files in the country folder
                for stock_file in os.listdir(country_path):
                    stock_path = os.path.join(country_path, stock_file)

                    if stock_file.endswith('.csv'):
                        ticker = os.path.splitext(stock_file)[0]

                        # Read the CSV file into a DataFrame
                        try:
                            df = pd.read_csv(stock_path, skiprows=2)

                            # Rename columns
                            df.columns = ['date', 'fwd_pe']

                            # Handle cases where 'eps' might be empty or data starts further down
                            df['fwd_pe'] = pd.to_numeric(df['fwd_pe'], errors='coerce')

                            # Add ticker and country columns
                            df['ticker'] = ticker
                            df['country'] = country_code
                            df['ticker_country'] = df['ticker'] + '_' + df['country']

                            # Reorder columns to match the SQL table
                            df = df[['ticker_country', 'ticker', 'country', 'date', 'fwd_pe']]

                            # Insert data into SQL table
                            df.to_sql('fwd_pe', conn, if_exists='append', index=False)
                        except Exception as e:
                            pass

    except Exception as e:
        pass

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("fwd_pe table data loaded successfully!")

def load_revenue_to_sql(db_path, factor_folder):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    revenue_folder = os.path.join(factor_folder, 'revenue')

    try:
        # Loop through the country code folders in the div_yield folder
        for country_code in os.listdir(revenue_folder):
            country_path = os.path.join(revenue_folder, country_code)

            if os.path.isdir(country_path):
                # Loop through the CSV files in the country folder
                for stock_file in os.listdir(country_path):
                    stock_path = os.path.join(country_path, stock_file)

                    if stock_file.endswith('.csv'):
                        ticker = os.path.splitext(stock_file)[0]

                        # Read the CSV file into a DataFrame
                        try:
                            df = pd.read_csv(stock_path, skiprows=2)

                            # Rename columns
                            df.columns = ['date', 'revenue']

                            # Handle cases where 'eps' might be empty or data starts further down
                            df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')

                            # Add ticker and country columns
                            df['ticker'] = ticker
                            df['country'] = country_code
                            df['ticker_country'] = df['ticker'] + '_' + df['country']

                            # Reorder columns to match the SQL table
                            df = df[['ticker_country', 'ticker', 'country', 'date', 'revenue']]

                            # Insert data into SQL table
                            df.to_sql('revenue', conn, if_exists='append', index=False)
                        except Exception as e:
                            pass

    except Exception as e:
        pass

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("Revenue table data loaded successfully!")

def load_roe_to_sql(db_path, factor_folder):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    roe_folder = os.path.join(factor_folder, 'roe')

    try:
        # Loop through the country code folders in the div_yield folder
        for country_code in os.listdir(roe_folder):
            country_path = os.path.join(roe_folder, country_code)

            if os.path.isdir(country_path):
                # Loop through the CSV files in the country folder
                for stock_file in os.listdir(country_path):
                    stock_path = os.path.join(country_path, stock_file)

                    if stock_file.endswith('.csv'):
                        ticker = os.path.splitext(stock_file)[0]

                        # Read the CSV file into a DataFrame
                        try:
                            df = pd.read_csv(stock_path, skiprows=2)

                            # Rename columns
                            df.columns = ['date', 'roe']

                            # Handle cases where 'eps' might be empty or data starts further down
                            df['roe'] = pd.to_numeric(df['roe'], errors='coerce')

                            # Add ticker and country columns
                            df['ticker'] = ticker
                            df['country'] = country_code
                            df['ticker_country'] = df['ticker'] + '_' + df['country']

                            # Reorder columns to match the SQL table
                            df = df[['ticker_country', 'ticker', 'country', 'date', 'roe']]

                            # Insert data into SQL table
                            df.to_sql('roe', conn, if_exists='append', index=False)
                        except Exception as e:
                            pass

    except Exception as e:
        pass

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("roe table data loaded successfully!")

def load_rsi_30d_to_sql(db_path, factor_folder):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    rsi_30d_folder = os.path.join(factor_folder, 'rsi_30d')

    try:
        # Loop through the country code folders in the div_yield folder
        for country_code in os.listdir(rsi_30d_folder):
            country_path = os.path.join(rsi_30d_folder, country_code)

            if os.path.isdir(country_path):
                # Loop through the CSV files in the country folder
                for stock_file in os.listdir(country_path):
                    stock_path = os.path.join(country_path, stock_file)

                    if stock_file.endswith('.csv'):
                        ticker = os.path.splitext(stock_file)[0]

                        # Read the CSV file into a DataFrame
                        try:
                            df = pd.read_csv(stock_path, skiprows=2)

                            # Rename columns
                            df.columns = ['date', 'rsi_30d']

                            # Handle cases where 'eps' might be empty or data starts further down
                            df['rsi_30d'] = pd.to_numeric(df['rsi_30d'], errors='coerce')

                            # Add ticker and country columns
                            df['ticker'] = ticker
                            df['country'] = country_code
                            df['ticker_country'] = df['ticker'] + '_' + df['country']

                            # Reorder columns to match the SQL table
                            df = df[['ticker_country', 'ticker', 'country', 'date', 'rsi_30d']]

                            # Insert data into SQL table
                            df.to_sql('rsi_30d', conn, if_exists='append', index=False)
                        except Exception as e:
                            pass

    except Exception as e:
        pass

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("rsi_30d table data loaded successfully!")

def load_fx_rates_to_sql(db_path, fx_folder):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    rates_folder = os.path.join(fx_folder, 'rates')

    try:
        # Loop through the CSV files in the rates folder
        for currency_file in os.listdir(rates_folder):
            file_path = os.path.join(rates_folder, currency_file)

            if currency_file.endswith('.csv'):
                combo_id = os.path.splitext(currency_file)[0]
                leg1 = combo_id[:3]
                leg2 = combo_id[4:]

                # Read the CSV file into a DataFrame
                try:
                    df = pd.read_csv(file_path, skiprows=2)

                    # Rename columns
                    df.columns = ['date', 'rate']

                    # Add combo_id, leg1, and leg2 columns
                    df['combo_id'] = combo_id
                    df['leg1'] = leg1
                    df['leg2'] = leg2

                    # Reorder columns to match the SQL table
                    df = df[['combo_id', 'date', 'leg1', 'leg2', 'rate']]

                    # Handle missing data by converting them to NaN which will be saved as NULL in SQL
                    df['rate'] = pd.to_numeric(df['rate'], errors='coerce')

                    # Insert data into SQL table
                    df.to_sql('fx_rates', conn, if_exists='append', index=False)
                except Exception as e:
                    pass

    except Exception as e:
        pass

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("FX rates table data loaded successfully!")

def load_index_to_sql(db_path, index_folder):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Loop through the CSV files in the index folder
        for index_file in os.listdir(index_folder):
            index_path = os.path.join(index_folder, index_file)

            if index_file.endswith('.csv'):
                _index = os.path.splitext(index_file)[0]

                # Read the CSV file into a DataFrame
                try:
                    df = pd.read_csv(index_path, skiprows=2)

                    if len(df.columns) < 6:
                        df = pd.read_csv(index_path, skiprows=1)
                        if 'PX_LAST' in df.columns:
                            # Extract 'PX_Last' as 'last' and keep 'date' from Column A
                            df = df.iloc[:, [0, df.columns.get_loc('PX_LAST')]]
                            df.columns = ['date', 'last']
                            df['open'] = None
                            df['high'] = None
                            df['low'] = None
                            df['volume'] = None
                        else:
                            print(f"File {index_file} is missing necessary columns")
                            continue  # Skip files without necessary columns
                    else:
                        df.columns = ['date', 'open', 'high', 'low', 'last', 'volume']

                    # Add _index column
                    df['_index'] = _index

                    # Reorder columns to match the SQL table
                    df = df[['_index', 'date', 'open', 'high', 'low', 'last', 'volume']]

                    # Insert data into SQL table
                    df.to_sql('_index', conn, if_exists='append', index=False)
                except Exception as e:
                    print(f"Error processing file {index_file}: {e}")

    except Exception as e:
        print(f"Error loading index data: {e}")

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("_index table data loaded successfully!")

# Define the database path and the folders
db_path = r'db_seed\cdb_lean_data.db'
equity_folder = 'equity'
factor_folder = 'factor'
fx_folder = 'fx'
index_folder = 'index'

# Load data into SQL tables
'''load_equity_to_sql(db_path, equity_folder)
load_div_yield_to_sql(db_path, factor_folder)
load_dw_score_to_sql(db_path, factor_folder)
load_eps_to_sql(db_path, factor_folder)
load_eps_nxt_yr_to_sql(db_path, factor_folder)
load_ev_ebit_to_sql(db_path, factor_folder)
load_fwd_pe_to_sql(db_path, factor_folder)
load_revenue_to_sql(db_path, factor_folder)
load_roe_to_sql(db_path, factor_folder)
load_rsi_30d_to_sql(db_path, factor_folder)
load_fx_rates_to_sql(db_path, fx_folder)'''
load_index_to_sql(db_path, index_folder)