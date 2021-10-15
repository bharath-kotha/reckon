import os

import psycopg2
from psycopg2 import OperationalError


def create_connection(
    db_name=None, db_user=None, db_password=None, db_host=None, db_port=None
):
    db_name = db_name or os.getenv("DB_NAME", "postgres")
    db_user = db_user or os.getenv("DB_USER", "postgres")
    db_password = db_password or os.getenv("DB_PASSWORD", "postgres")
    db_host = db_host or os.getenv("DB_HOST", "localhost")
    db_port = db_port or os.getenv("DB_PORT", 5432)
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


if __name__ == "__main__":
    connection = create_connection()

    # Trying creating a table, insert row and read
    cur = connection.cursor()

    CREATE_TABLE_QUERY = """ CREATE TABLE IF NOT EXISTS test_table(
            id serial PRIMARY KEY,
            value INTEGER
        )
    """

    cur.execute(CREATE_TABLE_QUERY)

    INSERT_QUERY = """ INSERT INTO test_table (value) VALUES (1) """

    cur.execute(INSERT_QUERY)

    SELECT_QUERY = """ SELECT * FROM test_table; """

    cur.execute(SELECT_QUERY)
    print(cur.fetchone())

    DELETE_QUERY = """ DROP TABLE test_table """
    cur.execute(DELETE_QUERY)

    connection.commit()
    cur.close()
    connection.close()
