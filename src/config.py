import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Path configurations
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

BRONZE_PATH = DATA_DIR / "bronze"
SILVER_PATH = DATA_DIR / "silver"
GOLD_PATH = DATA_DIR / "gold"

# File names
BRONZE_RAW_FILE = BRONZE_PATH / "bronze_layer_raw.csv"
SILVER_CLEAN_FILE = SILVER_PATH / "silver_layer_clean.parquet"

# S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('BUCKET_NAME')

# Data generation parameters
DATA_GEN_CONFIG = {
    "default_records": 100000,
    "default_duplicates": 5000
}