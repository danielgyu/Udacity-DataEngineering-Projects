import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads the staging tables by copying from the S3 bucket where raw data resides.

    Params
    ------
    cur : psycopg2 cursor
    conn : psycop2 connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data into the star and dimension tables from the staging tables.

    Params
    ------
    cur : psycopg2 cursor
    conn : psycop2 connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'insert complete : {query}')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
