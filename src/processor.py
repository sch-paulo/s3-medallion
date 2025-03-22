import os
import pandas as pd

def clean_silver_layer(df_source):
    """
    Clean and transform raw data into a standardized silver layer dataset.
    
    Parameters:
    df_source (DataFrame): Raw source data with quality issues
    
    Returns:
    DataFrame: Cleaned DataFrame with standardized formats, removed duplicates,
    filtered invalid values, and normalized fields
   """
    df = df_source.copy()
    
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

    df['status'] = df['status'].str.capitalize().replace(pd.NA, 'Unknown')

    return df.reset_index(drop=True)

if __name__ == '__main__':
    file_path_csv = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "bronze_layer_raw.csv")
    raw_df = pd.read_csv(file_path_csv)
    
    silver_df = clean_silver_layer(raw_df)
    file_path_parquet = os.path.join(os.path.dirname(__file__), "..", "..", "data", "processed", "silver_layer_clean.parquet")
    silver_df.to_parquet(file_path_parquet)
