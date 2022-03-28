import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from redshift import *



def load_staging_tables(cur, conn):
    count = 0
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        count += 1
        print('Staging query {}/2 success... \n'.format(count))
        print(query)

def insert_tables(cur, conn):
    count = 0
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        count += 1
        print('Inserting query {}/5 success... \n'.format(count))
        print(query)

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    DWH_ENDPOINT = config.get("DWH","DWH_ENDPOINT")
    conn = connect_database(config, DWH_ENDPOINT)
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    conn.close()
    iam, roleArn = create_iam_role(config)
    ec2, redshift = create_redshift_cluster(config, iam, roleArn)
    delete_redshift(config, redshift, iam)


if __name__ == "__main__":
    main()