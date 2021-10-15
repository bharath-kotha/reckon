#!/bin/python3
""" Test the performance of single BTree index

This file compares the performance of select query and insert query
when the index is present. We'll compare the performance with respect
to the number of rows.
"""

import random
from statistics import mean

from DB import create_connection
from psycopg2.extras import execute_values
from decorators import get_execution_time


def create_table(conn, table_name="test_table"):
    """ Create a table with the given table_name

    Creates the table with a primary key id, and another column value which is an integer.
    Also, if there's an existing table with the given name, the table is dropped

    Parameters
    1. conn - The connection object
    2. table_name - the name of the table to be created
    """

    DROP_TABLE_QUERY = f"""
        DROP TABLE IF EXISTS {table_name}
    """
    CREATE_TABLE_QUERY = f"""
        CREATE TABLE {table_name} (
            id serial PRIMARY KEY,
            value INTEGER
        )
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(DROP_TABLE_QUERY)
            cur.execute(CREATE_TABLE_QUERY)


def drop_table(conn, table_name="test_table"):
    """ Drop the table from the database if exists

    Parameters:
    1. conn - The connection object
    2. table_name - The name of the table to be dropped
    """
    DROP_TABLE_QUERY = f"""
        DROP TABLE IF EXISTS {table_name}
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(DROP_TABLE_QUERY)


def insert_single_row(conn, table_name="test_table", value=1):
    """ Insert the given value into the table

    The table is assumed to have only one column other than primary key which is
    an integer field.

    Parameters:
    1. conn - The connection object
    2. table_name - The name of the table to insert the value into
    3. value - The integer to be insert
    """
    INSERT_QUERY = f"""
        INSERT INTO {table_name} (value) VALUES (%s)
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(INSERT_QUERY, (value,))


def insert_multiple_rows(conn, table_name="test_table", values=None, batch_size=1000):
    """ Insert multiple values into the table

    The table is assumed to have only one column other than primary key which is
    an integer field.

    Parameters:
    1. conn - The connection object
    2. table_name - The name of the table to insert the value into
    3. values - The integer to be insert
    4. batch_size - The number of rows to insert at once
    """
    INSERT_QUERY = f"""
        INSERT INTO {table_name} (value) VALUES %s
    """
    value_tuples = [(val,) for val in values]
    with conn:
        with conn.cursor() as cur:
            execute_values(cur, INSERT_QUERY, value_tuples, page_size=batch_size)


def find_rows(conn, table_name="test_table", value=1):
    """ Find the row(s) with the given value in the table

    Parameters:
    1. conn - The connection object
    2. table_name - The name of the table to insert the value into
    3. value - The value to search
    """
    SELECT_QUERY = f"""
        SELECT * FROM {table_name} WHERE VALUE=%s
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_QUERY, value)


def create_index(conn, table_name="test_table", index_name=None):
    """ Creates index on a column named value in the given table

    Parameters:
    1. conn - The connection object
    2. table_name - The name of the table to create the index on
    3. index_name - The name of the index
    """
    index_name = index_name or f"{table_name}_value_idx"
    CREATE_INDEX_QUERY = f"""
        CREATE INDEX {index_name} ON {table_name} (value)
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_INDEX_QUERY)


def drop_index(conn, index_name=None):
    """ Drop the index with the given name

    Parameters:
    1. conn - The connection object
    2. index_name - The name of the index to drop
    """
    index_name = index_name or "test_table_value_idx"
    DROP_INDEX_QUERY = f"""
        DROP INDEX IF EXISTS {index_name}
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(DROP_INDEX_QUERY)


@get_execution_time
def create_rows(conn, num_rows=100, db_index=True, batch_size=1000):
    """ Create a table with the given number of rows

    Creates a new table with id and value columns and inserts
    the given number of rows. The value will change from 1 to
    the number of rows.

    Parameters:
    1. conn - The DB connection object
    2. num_rows - The number of rows to create
    3. db_index - Whether to create the DB index on the value column
    4. batch_size - The number of values to use in DB write at once
    """
    create_table(conn)
    if db_index:
        create_index(conn)
    insert_multiple_rows(conn, values=list(range(num_rows)), batch_size=batch_size)


@get_execution_time
def find_value(conn, value):
    """ Searches the value in the table

    Parameters:
    1. conn - The DB connection object
    2. value - The value to search for
    """
    find_rows(conn, value=(value,))


def metrics(conn, num_rows, num_experiments=1):

    write_times_with_index = []
    write_times_without_index = []
    find_time_with_index = []
    find_time_without_index = []
    print("---------------------------------------")
    print(f"Running {num_experiments} experiments with {num_rows}")
    print("-----------")
    for i in range(num_experiments):
        print(f"Running iteration {i}")
        print(f"Creating {num_rows} rows with index")
        t = create_rows(conn, db_index=True, num_rows=num_rows)
        write_times_with_index.append(t)
        print(f"Finding value in {num_rows} rows with index")
        t = find_value(conn, random.randint(1, num_rows))
        find_time_with_index.append(t)

        print(f"Creating {num_rows} rows without index")
        t = create_rows(conn, num_rows=num_rows, db_index=False)
        write_times_without_index.append(t)
        print(f"Finding value in {num_rows} rows without index")
        t = find_value(conn, random.randint(1, num_rows))
        find_time_without_index.append(t)
        print("-----------")

    print("")
    print(f"Average time to create rows with index {mean(write_times_with_index)}")
    print(
        f"Average time to create rows without index {mean(write_times_without_index)}"
    )
    print(f"Average time to find row with index {mean(find_time_with_index)}")
    print(f"Average time to find row without index {mean(find_time_without_index)}")

    print("")
    print(f"Write time with index raw data: {write_times_with_index}")
    print(f"Write time without index raw data: {write_times_without_index}")
    print(f"Find time with index raw data: {find_time_with_index}")
    print(f"Find time without index raw data: {find_time_without_index}")
    print("---------------------------------------")


if __name__ == "__main__":
    conn = create_connection()
    # Difference between selecting a row with/without indices
    # Parameters: 1. no. of rows
    # 1000 rows
    metrics(conn, 1000, 25)

    metrics(conn, 10_000, 25)

    metrics(conn, 100_000, 15)

    metrics(conn, 1_000_000, 15)

    metrics(conn, 10_000_000, 5)

    conn.close()
