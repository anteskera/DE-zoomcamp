FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt

RUN pip install -r docker-requirements.txt --trusted-host pypi.org --no-cache-dir

COPY flows /home/ante/Desktop/de-z/week2/etl_gcs_to_bq.py:etl_gcs_to_bq
COPY data /home/ante/Desktop/de-z/data