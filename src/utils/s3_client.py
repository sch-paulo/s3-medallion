import os
import boto3
import logging
from typing import List
from pathlib import Path
from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    BUCKET_NAME
)


logger = logging.getLogger(__name__)

class S3Uploader:
    def __init__(self) -> None:
        try:
            if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, BUCKET_NAME]):
                raise ValueError("Missing AWS credentials in environment variables")
            
            self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    region_name = AWS_REGION
                )
            self.s3_client.head_bucket(Bucket=BUCKET_NAME)
            logger.info(f"S3 client configured correctly")
        except Exception as e:
            logger.error(f"S3 client configuration error: {e}")
            raise
    
    def list_files(self, folder: Path) -> List[Path]:
        '''
        List all the files inside a local folder
        '''
        files = []
        try:
            for path in folder.iterdir():
                if path.is_file():
                    files.append(path)
            logger.info(f"Files listed in '{folder}': \n{files}")
            return files
            
        except NotADirectoryError as e:
            logger.error(f"Path {folder} is not a directory")
            raise
        
    
    def upload_files_to_s3(self, files: List[str], base_s3_key: str) -> None:
        '''
        Upload the listed files to S3 bucket
        '''
        for file in files:
            file_name = os.path.basename(file)
            try:
                logger.info(f"Trying to upload '{file_name}' to bucket '{BUCKET_NAME}'...")
                s3_key = base_s3_key + '/' + file_name
                self.s3_client.upload_file(file, BUCKET_NAME, s3_key)
                logger.info(f"'{file_name}' was sent to the path '{s3_key}' folder inside the S3 bucket '{BUCKET_NAME}'")
            except Exception as e:
                logger.error(f"Error sending '{file_name}' to S3: {e}")
                raise

if __name__ == '__main__':
    FOLDER = os.path.join('data', 'bronze')
    s3_uploader = S3Uploader()
    files = s3_uploader.list_files(Path('data/bronze'))
    # s3_uploader.upload_files_to_s3(files, 'bronze')
