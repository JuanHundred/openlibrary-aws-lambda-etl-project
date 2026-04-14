import json
import requests
import os
import logging
from s3_manager import S3Manager
from fetch import fetch_api

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
   bucket_name = os.getenv("BUCKET_NAME")
   s3_transformed_books = os.getenv("S3_TRANSFORMED_BOOKS")
   s3_covers = os.getenv("S3_COVERS_KEY")

   s3_manager = S3Manager(bucket_name)
   
   logger.info(f"Retrieving transformed books data from s3://{bucket_name}/{s3_transformed_books}")
   books_obj = s3_manager.get(s3_key=s3_transformed_books)
   books_data = json.loads(books_obj['Body'].read().decode('utf-8'))

   logger.info(f"Calling API and caching response at s3://{bucket_name}/{s3_covers}")
   fetch_and_cache_covers(books_data=books_data, s3_manager=s3_manager, s3_covers=s3_covers)

   logger.info("Process finished")
   
def fetch_and_cache_covers(books_data, s3_manager, s3_covers):
    """Calls API and cache the GET response."""
    covers_list = []

    # Get the json information of the cover for each book found
    for book in books_data:
        if book['cover_id']:
            cover_api_url = f"https://covers.openlibrary.org/b/id/{book['cover_id']}.json"
            raw_response = fetch_api(api_url=cover_api_url)
            if raw_response and raw_response.status_code == 200:
                covers_list.append(raw_response.json())

    # Only dump json if the json file is empty
    s3_manager.upload(s3_key=s3_covers, body=json.dumps(covers_list, indent=4, sort_keys=True).encode('UTF-8'))
