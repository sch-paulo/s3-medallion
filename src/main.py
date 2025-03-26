import pandas as pd
import logging
from typing import NoReturn
from pathlib import Path

from pipelines import data_generator, processor, aggregators
from utils.s3_client import S3Client
from config import (
    BRONZE_RAW_FILE,
    SILVER_CLEAN_FILE,
    DATA_GEN_CONFIG
)

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
    exit(1)

def run_pipeline() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Initializing an ETL pipeline for data ingestion into an S3 bucket using the Medallion Architecture")
    
    success = True
    
    try:
        s3_client = S3Client()
        # Generate raw messy data for bronze layer
        try:
            data_generator.generate_raw_data(
                DATA_GEN_CONFIG["default_records"],
                DATA_GEN_CONFIG["default_duplicates"]
            )
            files = s3_client.list_files(Path('data/bronze'))
            s3_client.upload_files_to_s3(files, 'bronze')
        except Exception as e:
            logger.error(f"Bronze layer generation failed: {str(e)}")
            success = False
            exit()

        # Clean raw data (silver layer)
        try:
            s3_client.download_files_from_s3('bronze/', 'data/bronze')
            raw_df = pd.read_csv(BRONZE_RAW_FILE)
            processor.clean_raw_data(raw_df)
            files = s3_client.list_files(Path('data/silver'))
            s3_client.upload_files_to_s3(files, 'silver')
        except FileNotFoundError as e:
            logger.error(f"Input file not found: {str(e)}")
            success = False
            exit()
        except Exception as e:
            logger.error(f"Silver layer processing failed: {str(e)}")
            success = False
            exit()

        # Generate gold layer aggregated DataFrames
        try:
            s3_client.download_files_from_s3('silver/', 'data/silver')
            silver_df = pd.read_parquet(SILVER_CLEAN_FILE)
            aggregators.create_gold_layer(silver_df)
            files = s3_client.list_files(Path('data/gold'))
            s3_client.upload_files_to_s3(files, 'gold')
        except Exception as e:
            logger.error(f"Gold layer creation failed: {str(e)}")
            success = False  
            exit()

    except Exception as e:
        logger.error(f"Pipeline aborted: {str(e)}")
        success = False
        exit()

    if success:
        logger.info("Pipeline was executed gracefully!")


if __name__ == '__main__':
    run_pipeline()