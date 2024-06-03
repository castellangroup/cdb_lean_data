import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    """Create a database connection to a SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connection to {db_file} established.")
    except Error as e:
        print(f"Error: {e}")
    return conn

def create_table(conn, create_table_sql):
    """Create a table from the create_table_sql statement."""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        print("Table created successfully.")
    except Error as e:
        print(f"Error: {e}")

def main():
    database = "cdb_lean_data.db"

    sql_create_prices_table = """
    CREATE TABLE prices (
        ticker_country TEXT NOT NULL,
        ticker TEXT NOT NULL,
        country TEXT NOT NULL,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        last REAL,
        volume REAL
    );
    """
    
    sql_create_div_yield_table = """
    CREATE TABLE div_yield (
        ticker_country TEXT NOT NULL,
        ticker TEXT NOT NULL,
        country TEXT NOT NULL,
        date TEXT NOT NULL,
        dividend_yield REAL NULL
    );
    """

    sql_create_dw_score_table = """
    CREATE TABLE dw_score (
        ticker_country TEXT NOT NULL,
        ticker TEXT NOT NULL,
        country TEXT NOT NULL,
        date TEXT NOT NULL,
        dw_score REAL NULL
    );
    """

    sql_create_eps_table = """
    CREATE TABLE eps (
        ticker_country TEXT NOT NULL,
        ticker TEXT NOT NULL,
        country TEXT NOT NULL,
        date TEXT NOT NULL,
        eps REAL NULL
    );
    """

    sql_create_eps_nxt_yr_table = """
    CREATE TABLE eps_nxt_yr (
        ticker_country TEXT NOT NULL,
        ticker TEXT NOT NULL,
        country TEXT NOT NULL,
        date TEXT NOT NULL,
        eps_nxt_yr REAL NULL
    );
    """

    sql_create_ev_ebit_table = """
    CREATE TABLE ev_ebit (
        ticker_country TEXT NOT NULL,
        ticker TEXT NOT NULL,
        country TEXT NOT NULL,
        date TEXT NOT NULL,
        ev_ebit REAL NULL
    );
    """

    sql_create_fwd_pe_table = """
    CREATE TABLE fwd_pe (
        ticker_country TEXT NOT NULL,
        ticker TEXT NOT NULL,
        country TEXT NOT NULL,
        date TEXT NOT NULL,
        fwd_pe REAL NULL
    );
    """

    sql_create_revenue_table = """
    CREATE TABLE revenue (
        ticker_country TEXT NOT NULL,
        ticker TEXT NOT NULL,
        country TEXT NOT NULL,
        date TEXT NOT NULL,
        revenue REAL NULL
    );
    """

    sql_create_roe_table = """
    CREATE TABLE roe (
        ticker_country TEXT NOT NULL,
        ticker TEXT NOT NULL,
        country TEXT NOT NULL,
        date TEXT NOT NULL,
        roe REAL NULL
    );
    """

    sql_create_rsi_30d_table = """
    CREATE TABLE rsi_30d (
        ticker_country TEXT NOT NULL,
        ticker TEXT NOT NULL,
        country TEXT NOT NULL,
        date TEXT NOT NULL,
        rsi_30d REAL NULL
    );
    """

    sql_create_fx_rates_table = """
    CREATE TABLE fx_rates (
        combo_id TEXT NOT NULL,
        date TEXT NOT NULL,
        leg1 TEXT NOT NULL,
        leg2 TEXT NOT NULL,
        rate REAL NULL
    );
    """

    sql_create_index_table = """
    CREATE TABLE _index (
        _index TEXT NOT NULL,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        last REAL,
        volume REAL
    );
    """

    # Create a database connection
    conn = create_connection(database)

    # Create tables
    if conn is not None:
        create_table(conn, sql_create_prices_table)
        create_table(conn, sql_create_div_yield_table)
        create_table(conn, sql_create_dw_score_table)
        create_table(conn, sql_create_eps_table)
        create_table(conn, sql_create_eps_nxt_yr_table)
        create_table(conn, sql_create_ev_ebit_table)
        create_table(conn, sql_create_fwd_pe_table)
        create_table(conn, sql_create_revenue_table)
        create_table(conn, sql_create_roe_table)
        create_table(conn, sql_create_rsi_30d_table)
        create_table(conn, sql_create_fx_rates_table)
        create_table(conn, sql_create_index_table)
    else:
        print("Error! Cannot create the database connection.")

    # Close the connection
    if conn:
        conn.close()

if __name__ == '__main__':
    main()
