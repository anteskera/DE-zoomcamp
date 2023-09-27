from pathlib import Path
import pandas as pd
from prefect import flow,task
from random import randint
import os 
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3, cache_key_fn = task_input_hash, cache_expiration=timedelta(days=1))
def fetch(file_name: str) -> pd.DataFrame:
    
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    os.system(f"wget {dataset_url} -O {file_name}")
    df = pd.read_parquet(dataset_url)
    return df 

@task()
def clean( df = pd.DataFrame) -> pd.DataFrame:
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color:str, dataset_file:str) -> Path:
    path = Path(f"{color}/{dataset_file}")
    df.to_parquet(path, compression="gzip")

    return path


@task()
def write_gcs(path: Path):
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-zoom")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)
                                                    
    return 

@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    dataset_file = f"{color}_tripdata_{year}-{month:02d}.parquet"

    df = fetch(dataset_file)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)
    

@flow()
def etl_parent_flow(year: int = 2021, months: list[int] = [1,2], color: str = 'yellow'):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == '__main__':
    color = 'yellow'
    year = 2021
    months = [1, 2, 3]
    etl_parent_flow(year, months, color)
        