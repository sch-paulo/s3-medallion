import pandas as pd
import logging
from typing import NoReturn
from pathlib import Path

from pipelines import data_generator, processor, aggregators
from utils.s3_client import S3Uploader
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
        # Generate raw messy data for bronze layer
        try:
            data_generator.generate_raw_data(
                DATA_GEN_CONFIG["default_records"],
                DATA_GEN_CONFIG["default_duplicates"]
            )
        except Exception as e:
            logger.error(f"Bronze layer generation failed: {str(e)}")
            success = False
            exit()

        # Clean raw data (silver layer)
        try:
            raw_df = pd.read_csv(BRONZE_RAW_FILE)
            processor.clean_raw_data(raw_df)
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
            silver_df = pd.read_parquet(SILVER_CLEAN_FILE)
            aggregators.create_gold_layer(silver_df)
        except Exception as e:
            logger.error(f"Gold layer creation failed: {str(e)}")
            success = False  
            exit()

        # S3 upload
        try:
            s3_uploader = S3Uploader()
            for folder in ['bronze', 'silver', 'gold']:
                files = s3_uploader.list_files(Path('data') / folder)
                s3_uploader.upload_files_to_s3(files, folder)
        except Exception as e:
            logger.error(f"S3 upload failed: {str(e)}")
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