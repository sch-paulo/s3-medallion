import os
import sys
import boto3
import logging
from typing import List
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    BUCKET_NAME
)

logger = logging.getLogger(__name__)

class S3Client:
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
        List all files in the specified directory.
    
        Parameters:
        folder (Path): Directory path to search for files. Must be a valid directory.
        
        Returns:
        List[Path]: List of Path objects representing each file found in the directory.
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
        Upload multiple local files to an S3 bucket under the specified base key.
    
        Parameters:
        files (List[str]): List of local file paths to upload
        base_s3_key (str): Base S3 key path where files will be stored. 
        
        Returns:
        None: The function uploads files to S3 but doesn't return a value
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

    def download_files_from_s3(self, s3_prefix: str, local_dir: str) -> None:
        """
        Download files from an S3 bucket matching a prefix to a local directory.
    
        Recursively downloads all objects under the specified prefix while preserving 
        directory structure relative to the prefix. Creates local directories as needed.
        
        Parameters:
        s3_prefix (str): S3 prefix/path specifying which objects to download
        local_dir (str): Local directory path where files will be saved
        
        Returns:
        None: The function saves files locally but doesn't return a value
        """
        try:
            logger.info(f"Trying to download files from '{s3_prefix}'...")
            Path(local_dir).mkdir(parents=True, exist_ok=True)
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=s3_prefix)

            for page in page_iterator:
                if 'Contents' not in page:
                    continue
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('/'):
                        continue
                    
                    relative_path = key[len(s3_prefix):].lstrip('/')
                    if not relative_path:
                        relative_path = os.path.basename(key)
                    local_file_path = os.path.join(local_dir, relative_path)
                    
                    Path(local_file_path).parent.mkdir(parents=True, exist_ok=True)
                    
                    self.s3_client.download_file(BUCKET_NAME, key, local_file_path)
                    logger.info(f"Downloaded '{key}' to '{local_file_path}'")
                    
        except Exception as e:
            logger.error(f"Error downloading files from S3: {e}")
            raise

if __name__ == '__main__':
    FOLDER = os.path.join('data', 'bronze')
    s3_client = S3Client()
    # files = s3_client.list_files(Path('data/bronze'))
    # s3_client.upload_files_to_s3(files, 'bronze')
    s3_client.download_files_from_s3(
        s3_prefix='bronze/',  # The S3 folder to download from
        local_dir='data/bronze'  # Local directory to save files
    )
