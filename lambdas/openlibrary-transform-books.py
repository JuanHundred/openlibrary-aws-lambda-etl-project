import json
import os
import logging
import polars as pl
import io
from s3_manager import S3Manager 

logger = logging.getLogger()
logger.setLevel(logging.INFO)   

def lambda_handler(event, context):
    bucket_name = os.getenv('BUCKET_NAME')
    s3_books_key = os.getenv('S3_BOOKS_KEY')
    s3_json_key = os.getenv('S3_BOOKS_JSON_KEY')   
    s3_csv_key = os.getenv('S3_BOOKS_CSV_KEY')
    s3_parquet_key = os.getenv('S3_BOOKS_PARQUET_KEY')
    s3_processed_key = os.getenv('S3_PROCESSED_KEY')     

    s3_manager = S3Manager(bucket_name)
    #logger.info(f"Fetching object s3//{bucket_name}/{s3_books_key}")
    book_obj = s3_manager.get(s3_key=s3_books_key)
    logger.info(f"Reading the data from {s3_books_key}")
    raw_data = json.loads(book_obj['Body'].read().decode('utf-8'))

    # Parse and transform the data
    logging.info(f"Parsing {len(raw_data['docs'])} books")
    books_data = parse_books(books=raw_data)

    # Upload the transformed data to S3
    # Create df
    logging.info("Creating a dataframe and uploading to S3")
    books_df = pl.DataFrame(books_data)

    #  Upload as a json object
    logging.info(f"Uploading the json object to s3://{bucket_name}/{s3_json_key}")
    s3_manager.upload(
        s3_key=s3_json_key,
        body=books_df.write_json().encode('UTF-8')
        )

    # Upload as a csv object
    logging.info(f"Uploading the csv object to s3://{bucket_name}/{s3_csv_key}")
    s3_manager.upload(
        s3_key=s3_csv_key,
        body=books_df.write_csv().encode('UTF-8')
        )

    # Upload as a parquet object
    buffer = io.BytesIO()
    books_df.write_parquet(buffer)
    buffer.seek(0)
    logger.info(f"Uploading the parquet object to s3://{bucket_name}/{s3_parquet_key}")
    s3_manager.upload(
        s3_key=s3_parquet_key,
        body=buffer.getvalue()
        )

    # Move and delete the raw data that was just processed
    logger.info(f"Moving the raw data from s3://{bucket_name}/{s3_books_key} to s3://{bucket_name}/{s3_processed_key}")
    s3_manager.move(
        s3_key=s3_books_key,
        dest_key=s3_processed_key
        )
    logger.info("Process finished")
def parse_books(books)-> list:
    """Returns a list of dictionaries of the parsed json file."""
    docs = books['docs']
    books_data = list()
    for book_info in docs:
        if 'author_name' in book_info.keys():
            for i in range(len(book_info['author_name'])):
                book = dict()
                book['book_id'] = book_info['key']
                book['book_title'] = book_info['title']
                book['book_author'] = book_info['author_name'][i]
                book['published_year'] = check_publish_year(book_info=book_info)
                book['languages'] = check_number_of_languages(book_info=book_info)
                book['cover_id'] = check_cover_id(book_info=book_info)
                books_data.append(book)
        else:
            book = dict()
            book['book_id'] = book_info['key']
            book['book_title'] = book_info['title']
            book['published_year'] = check_publish_year(book_info=book_info)
            book['languages'] = check_number_of_languages(book_info=book_info)
            book['cover_id'] = check_cover_id(book_info=book_info)
            books_data.append(book)
    return books_data

def check_publish_year(book_info)-> int | None:
    """Returns the published year if present, else None. Used to skip first plush year API calls."""
    if 'first_publish_year' in book_info.keys():
        return book_info['first_publish_year']
    return 0

def check_number_of_languages(book_info)-> int | None:
    """Returns the number of languages if present, else None. Used to skip language API calls."""
    if 'language' in book_info.keys():
        return len(book_info['language'])
    return 0

def check_cover_id(book_info)-> int | None:
    """Returns the cover ID if present, else None. Used to skip cover API calls."""
    if 'cover_i' in book_info.keys():
        return book_info['cover_i']
    return None