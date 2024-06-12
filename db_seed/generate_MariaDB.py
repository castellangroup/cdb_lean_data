import pandas as pd
import mariadb
from mariadb import Error

def create_connection(host, user, password, database):
    """Create a database connection to a MariaDB database."""
    conn = None
    try:
        conn = mariadb.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        print(f"Connection to MariaDB established.")
    except Error as e:
        print(f"Error: {e}")
    return conn

def create_table(conn, create_table_sql):
    """Create a table from the create_table_sql statement."""
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        print("Table created successfully.")
    except Error as e:
        print(f"Error: {e}")

def main():
    host = "127.0.0.1"
    user = "root"
    password = "CGtest"
    database = "upload_try"

    sql_create_div_yield_table = """
    CREATE TABLE div_yield (
        ticker_country TEXT NOT NULL,
        ticker TEXT NOT NULL,
        country TEXT NOT NULL,
        date TEXT NOT NULL,
        dividend_yield REAL NULL
    );
    """

    # Create a database connection
    conn = create_connection(host, user, password, database)

    # Create the div_yield table
    if conn is not None:
        create_table(conn, sql_create_div_yield_table)
    else:
        print("Error! Cannot create the database connection.")

    # Close the connection
    if conn:
        conn.close()

if __name__ == '__main__':
    main()
