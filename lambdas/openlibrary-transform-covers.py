import json
import os
import io
import logging
import polars as pl
from s3_manager import S3Manager

logger = logging.getLogger()
logger.setLevel(logging.INFO)   

def lambda_handler(event, context):
    bucket_name = os.getenv("BUCKET_NAME")
    s3_csv_key = os.getenv("S3_COVERS_CSV_KEY")
    s3_json_key = os.getenv("S3_COVERS_JSON_KEY")
    s3_parquet_key = os.getenv("S3_COVERS_PARQUET_KEY")
    s3_covers_key = os.getenv("S3_COVERS_KEY")
    s3_procesed_key = os.getenv("S3_PROCESSED_KEY")

    s3_manager = S3Manager(bucket_name)
    # Get the covers object from to_process
    covers = s3_manager.get(s3_key=s3_covers_key)
    logger.info(f"Reading data from {s3_covers_key}")
    covers_raw_data = json.loads(covers['Body'].read().decode('utf-8'))

    # Transform the covers data
    covers_data = parse_covers(covers_raw_data)
    
    logging.info("Creating a dataframe and uploading to S3")
    # Create dataframe of covers_data
    covers_df = pl.from_dicts(covers_data)

    # Add the transformed data to an S3 bucket as a json object
    logging.info(f"Uploading the json object to s3://{bucket_name}/{s3_json_key}")
    s3_manager.upload(
        s3_key=s3_json_key, 
        body=covers_df.write_json().encode('UTF-8')
        )

    # Add the transformaed data to an S3 bucket as a csv object
    logging.info(f"Uploading the csv object to s3://{bucket_name}/{s3_csv_key}")
    s3_manager.upload(
        s3_key=s3_csv_key,
        body=covers_df.write_csv().encode('UTF-8')
        )
    
    # Add the transformaed data to an S3 bucket as a parquet object
    buffer = io.BytesIO()
    covers_df.write_parquet(buffer)
    buffer.seek(0)
    logging.info(f"Uploading the parquet object to s3://{bucket_name}/{s3_parquet_key}")
    s3_manager.upload(
        s3_key=s3_parquet_key,
        body=buffer.getvalue()
        )

    # Move and delete the raw processed data
    logger.info(f"Moving the raw data from s3://{bucket_name}/{s3_covers_key} to s3://{bucket_name}/{s3_procesed_key}")
    s3_manager.move(
        s3_key=s3_covers_key,
        dest_key=s3_procesed_key
        )

    logger.info("Process finished")
def parse_covers(covers)-> list:
    """Returns data from the covers.json file."""
    covers_data = list()

    for cover in covers:
        covers_info = {}
        covers_info['id'] = cover['id']
        covers_info['cover_url'] = f"https://covers.openlibrary.org/b/id/{cover['id']}.jpg"
        covers_info['cover_height'] = cover['height']
        covers_info['cover_width'] = cover['width']
        covers_data.append(covers_info)

    return covers_data