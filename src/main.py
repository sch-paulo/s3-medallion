import pandas as pd
import logging
from typing import NoReturn
from pathlib import Path

from pipelines import data_generator, processor, aggregators
from utils.s3_client import S3Uploader

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/pipeline.log'),
            logging.StreamHandler()
        ]
    )

def log_exception(msg: str) -> NoReturn:
    logging.critical(msg)
    # Optional: Send alert via email/Slack
    exit(1)

def run_pipeline() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info(f"Initializing an ETL pipeline for data ingestion into an S3 bucket using the Medallion Architecture")
    
    try:
    # Generate raw messy data for bronze layer
        try:
            data_generator.generate_raw_data(10000, 120)
        except Exception as e:
            log_exception(f"Bronze layer generation failed: {str(e)}")
    
    # Clean raw data (silver layer)
        try:
            raw_df = pd.read_csv('data/bronze/bronze_layer_raw.csv')
            processor.clean_raw_data(raw_df)
        except FileNotFoundError as e:
            log_exception(f"Input file not found: {str(e)}")
        except Exception as e:
            log_exception(f"Silver layer processing failed: {str(e)}")
    
    # Generate gold layer aggregated DataFrames
        try:
            silver_df = pd.read_parquet('data/silver/silver_layer_clean.parquet')
            aggregators.create_gold_layer(silver_df)
        except Exception as e:
            log_exception(f"Gold layer creation failed: {str(e)}")  
    
    # S3 upload
        try:
            s3_uploader = S3Uploader()
            for folder in ['bronze', 'silver', 'gold']:
                files = s3_uploader.list_files(Path('data') / folder)
                s3_uploader.upload_files_to_s3(files, folder)
        except Exception as e:
            log_exception(f"S3 upload failed: {str(e)}")

        logger.info(f"Pipeline was executed gracefully!")

    except Exception as e:
        log_exception(f"Pipeline aborted: {str(e)}")    


if __name__ == '__main__':
    run_pipeline()