import os
from typing import List
import boto3
from dotenv import load_dotenv

BUCKET_NAME = 'etl'

files = {'raw': ['src\\..\\data\\raw\\bronze_layer_raw.csv'], 'processed': ['src\\..\\data\\processed\\silver_layer_clean.parquet'], 'curated': []}

for layer in files.keys():
    for file in files[layer]:
        file_name = os.path.basename(file)
        print(file_name)
        try:
            print(f"Trying to upload '{file_name}' to bucket '{BUCKET_NAME}'...")
            print(f'Bucket name: {BUCKET_NAME + "/" + layer}')
        #     s3_client.upload_file(layer, BUCKET_NAME, file_name)
            print(f'{file_name} was sent to the S3 bucket {BUCKET_NAME}')
        except Exception as e:
            print(f"Error sending '{file_name}' to S3: {e}")
            raise