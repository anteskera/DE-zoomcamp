import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    file_name = 'output.parquet'
    os.system(f"wget {url} -O {file_name}")
    df = pd.read_parquet(file_name)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(pd.io.sql.get_schema(df, name=table_name))
        
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    chunk_size = 100000
    for chunk_start in range(0, len(df), chunk_size):
        t_start = time()
        
        chunk = df.iloc[chunk_start:chunk_start + chunk_size]
        chunk.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        print('inserted another chunk ..., took %.3f seconds' % (t_end - t_start))
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', type=int, help='Postgres port')
    parser.add_argument('--db', help='postgres database name')
    parser.add_argument('--table_name', help='postgres table name')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)