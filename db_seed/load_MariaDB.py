import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def create_engine_connection(user, password, host, port, database):
    """Create a database connection to a MariaDB database using SQLAlchemy."""
    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    try:
        engine = create_engine(connection_string)
        print(f"Connection to MariaDB established.")
        return engine
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None

def load_div_yield_to_sql(engine, factor_folder):
    """Load dividend yield data into the div_yield table."""
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
                            df.to_sql('div_yield', engine, if_exists='append', index=False)
                        except Exception as e:
                            print(f"Error processing file {stock_path}: {e}")

    except Exception as e:
        print(f"Error: {e}")

    print("Dividend yield table data loaded successfully!")

def main():
    host = "127.0.0.1"
    user = "root"
    password = "CGtest"
    port = "3306"  # Default port for MariaDB
    database = "upload_try"
    factor_folder = "factor"

    # Create a database connection
    engine = create_engine_connection(user, password, host, port, database)

    # Load dividend yield data into the div_yield table
    if engine is not None:
        load_div_yield_to_sql(engine, factor_folder)
    else:
        print("Error! Cannot create the database connection.")

if __name__ == '__main__':
    main()
