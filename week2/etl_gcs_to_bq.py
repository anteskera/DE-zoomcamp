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
    gcs_block.get_directory(from_path=gcs_path, to_path=f"{color}")
    return Path(f"color")

@flow()
def etl_gcs_to_bq():
    """Main ETL flow to lad data from GCS to BigQuery"""
    color = 'yellow'
    year = 2021
    month = 1
    path = extract_from_gcs(color, year, month)
    