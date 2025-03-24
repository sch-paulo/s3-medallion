import os
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def clean_raw_data(bronze_df: pd.DataFrame, file_name: str = 'silver_layer_clean') -> None:    
    '''
    Clean raw bronze layer data and save the result as a silver layer parquet file.
    
    Parameters:
    bronze_df (DataFrame): Raw bronze layer data with quality issues
    file_name (str, optional): Name for the output parquet file. Defaults to 'silver_layer_clean'
    
    Returns:
    None: The function saves the cleaned data to a parquet file but doesn't return a value
    '''
    try:
        required_columns = {'name', 'email', 'phone', 'age', 'salary', 'address', 'signup_date', 'status'}
        if not required_columns.issubset(bronze_df.columns):
            missing = required_columns - set(bronze_df.columns)
            raise ValueError(f"Input DataFrame missing columns: {missing}")
        df = bronze_df.copy()
        
        df = df.dropna(subset=['name'])
        df['name'] = df['name'].str.strip()

        df = df.drop_duplicates(subset=['email'], keep='first')
        email_mask = df['email'].str.contains(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        df = df[email_mask]
        
        df['phone'] = df['phone'].str.replace(r'\D', '', regex=True)
        df['phone'] = df['phone'].str.replace(r'^(\d{3})(\d{3})(\d{4})$', r'(\1) \2-\3', regex=True)
        
        df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(-1).astype(int)
        df = df[df['age'].between(18, 75)]

        df['salary'] = df['salary'].clip(lower=0, upper=200000)

        df['address'] = df['address'].replace(r'^\s*$', 'Unknown', regex=True)
        df['address'] = df['address'].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        df['signup_date'] = df['signup_date'].str.replace('/', '-')
        df['signup_date'] = pd.to_datetime(df['signup_date'], format='mixed', errors='coerce').dt.strftime('%Y-%m-%d')
        df = df.dropna(subset=['signup_date'])

        df['status'] = (df['status'].str.strip().str.capitalize().replace(['', pd.NA], 'Unknown'))

        output_path = Path(f'data/silver/{file_name}.parquet')
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.reset_index(drop=True).to_parquet(output_path)

        if df['email'].isnull().any():
            raise ValueError("Null emails present after cleaning")
            
        output_path = Path(f'data/silver/{file_name}.parquet')
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path)
        
        logger.info(f"Successfully cleaned data to {output_path}")

    except (ValueError, pd.errors.ParserError) as e:
        logger.error(f"Data cleaning failed: {str(e)}")
        raise
    except Exception as e:
        logger.exception("Unexpected error during data cleaning")
        raise RuntimeError("Data cleaning failed") from e


if __name__ == '__main__':
    file_path_csv = os.path.join(os.path.dirname(__file__), "..", "..", "data", "bronze", "bronze_layer_raw.csv")
    raw_df = pd.read_csv(file_path_csv)
    
    silver_df = clean_raw_data(raw_df)
