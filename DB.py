import os

import psycopg2
from psycopg2 import OperationalError

db_name = os.getenv("DB_NAME", "postgres")
db_user = os.getenv("DB_USER", "postgres")
db_password = os.getenv("DB_PASSWORD", "postgres")
db_host = os.getenv("DB_HOST", "localhost")
db_port = os.getenv("DB_PORT", 5432)


def create_connection(db_name, db_user, db_password, db_host, db_port):
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


connection = create_connection(db_name, db_user, db_password, db_host, db_port)

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
