import requests
import json
import os
import logging
from s3_manager import S3Manager
from fetch import fetch_api

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Get environment variables
    api_url = os.getenv("API_URL")
    bucket_name = os.getenv("BUCKET_NAME")
    s3_books_key = os.getenv("S3_BOOKS_KEY")

    # Get raw data from open library API searching for Harry Potter books
    logger.info(f"Fetching data from {api_url}")
    raw_response = fetch_api(api_url=api_url)
    logger.info(f"Data fetched. Status code: {raw_response.status_code}")
    
    s3_manager = S3Manager(bucket_name)
    logger.info("Uploading to S3")
    cache_books_response(raw_response, s3_books_key, s3_manager)
    logger.info("Process finished")

def cache_books_response(raw_response, s3_key, s3_manager):
    """Cache's API GET result."""
    if raw_response and raw_response.status_code == 200:
        s3_manager.upload(s3_key=s3_key, body=json.dumps(raw_response.json(), indent=4, sort_keys=True).encode('UTF-8'))