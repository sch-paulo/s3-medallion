import os
from typing import List
import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('BUCKET_NAME')

try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name = AWS_REGION
    )
    print('S3 client configured correctly')
except Exception as e:
    print(f'S3 client configuration error: {e}')
    raise

def list_files(folder):
    '''
    List all the files inside a local folder
    '''
    layer_files = {'raw':[], 'processed':[], 'curated':[]}
    try:
        for layer in layer_files.keys():
            for file_name in os.listdir(folder + '\\' + layer):
                full_path = os.path.join(folder, layer, file_name)
                if os.path.isfile(full_path):
                    layer_files[layer].append(full_path)
        print(f"Files listed in '{folder}': \n{layer_files}")
    except Exception as e:
        print(f"Error listing files in'{folder}': {e}")
        raise
    return layer_files

def upload_files_to_s3(files):
    '''
    Upload the listed files to S3 bucket
    '''
    for layer in files.keys():
        for file in files[layer]:
            file_name = os.path.basename(file)
            try:
                print(f"Trying to upload '{file_name}' to bucket '{BUCKET_NAME}'...")
                s3_key = layer + "/" + file_name
                s3_client.upload_file(file, BUCKET_NAME, s3_key)
                print(f"'{file_name}' was sent to the '{layer}' folder inside the S3 bucket '{BUCKET_NAME}'")
            except Exception as e:
                print(f"Error sending '{file_name}' to S3: {e}")
                raise

if __name__ == '__main__':
    FOLDER = r'src\..\data'
    files = list_files(FOLDER)
    upload_files_to_s3(files)