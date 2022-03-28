import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from redshift import *



def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print('[>] Successful drop tables \n')

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print('[>] Successful create tables \n')

def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    #DWH_PORT = config.get('DWH', 'DWH_PORT')
    #DWH_DB = config.get('DWH', 'DWH_DB')
    DWH_ENDPOINT = config.get("DWH","DWH_ENDPOINT")
    conn = connect_database(config, DWH_ENDPOINT)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()