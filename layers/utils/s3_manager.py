import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class S3Manager:
    def __init__(self, bucket_name):
        self.s3_client = boto3.client('s3')
        self.bucket = bucket_name

    def upload(self, s3_key, body):
        """Uploads files to S3."""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=(body)
                )
            logger.info(f"Uploaded to s3://{self.bucket}/{s3_key}")
        except ClientError as e:
            logger.error(f"Failed to upload to {s3_key}: {e}", exc_info=True)

    def get(self, s3_key):
        """Gets object from S3."""
        try:
            logger.info(f"Trying to retrieve s3://{self.bucket }/{s3_key}")
            return self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
        except ClientError as e:
            logger.error(f"Failed to upload to {s3_key}: {e}", exc_info=True)
        
    def copy(self, source_key, dest_key):
        """Copies object from one destination to another."""
        try:
            self.s3_client.copy_object(
                Bucket=self.bucket,
                Key=dest_key,
                CopySource={'Bucket': self.bucket, 'Key': source_key}
            )
            logger.info(f"Copied object {source_key} -> {dest_key}")
        except ClientError as e:
            logger.error(f"Failed to copy {source_key} to {dest_key}: {e}")
    
    def delete(self, s3_key):
        """Moves an object to a new destination."""
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=s3_key)
            logger.info(f"Deleted {s3_key}")
        except ClientError as e:
            logger.error(f"Failed to delete {s3_key}: {e}")
    
    def move(self, s3_key, dest_key):
        """Copies and delete object in the same bucket"""
        self.copy(s3_key, dest_key)
        self.delete(s3_key)

        
