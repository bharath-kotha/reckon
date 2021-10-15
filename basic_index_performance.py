#!/bin/python3
""" Test the performance of single BTree index

This file compares the performance of select query and insert query
when the index is present. We'll compare the performance with respect
to the number of rows.
"""

from DB import create_connection
from psycopg2.extras import execute_values


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


if __name__ == "__main__":
    conn = create_connection()
    conn.close()
