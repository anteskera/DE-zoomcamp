from pathlib import Path
import pandas as pd
from prefect import flow,task
from random import randint
import os 
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task()
def extract_from_gcs(color:str, year:int, month:int) -> Path:
    """"Download trip data from GCS"""
    gcs_path = f"{color}/{color}_tripdata_{year}-{month:02d}.parquet"
    gcs_block = GcsBucket.load("gcs-zoom")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./data")
    return Path(f"./data/{gcs_path}")

def transform(path: Path):
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df.passenger_count.isna().sum()}")
    df['passenger_count'] = df.passenger_count.fillna(0)
    print(f"post: missing passenger count: {df.passenger_count.isna().sum()}")

    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials = GcpCredentials.load("gcp-credentials")

    df.to_gbq(
        destination_table="de_zoomcamp.rides",
        project_id="glossy-ally-396206",
        credentials=gcp_credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to lad data from GCS to BigQuery"""
    color = 'yellow'
    year = 2021
    month = 1
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)

if __name__ == '__main__':
    etl_gcs_to_bq()