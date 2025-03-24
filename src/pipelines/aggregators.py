import os
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def create_gold_layer(silver_df):
    '''
    Transform silver layer data into gold layer analytics-ready data and save as CSV files.
    
    Parameters:
    silver_df (DataFrame): Cleaned silver layer data from the silver layer
    
    Returns:
    None: The function saves multiple analytics datasets to CSV files but doesn't return a value
    '''
    try:
        if silver_df.empty:
            raise ValueError("Input DataFrame is empty")
        
        required_cols = ['status', 'signup_date', 'age', 'salary', 'email']
        if not set(required_cols).issubset(silver_df.columns):
            missing = set(required_cols) - set(silver_df.columns)
            raise ValueError(f"Missing required columns: {missing}")
        
        df = silver_df.copy()
        
        df['signup_date'] = pd.to_datetime(df['signup_date'])
        
        # Active users analysis
        active_users = df[df['status'] == 'Active'].copy()
        active_users['signup_year'] = active_users['signup_date'].dt.year
        active_users['signup_month'] = active_users['signup_date'].dt.month
        active_users['age_group'] = pd.cut(
            active_users['age'], 
            bins=[18, 25, 35, 45, 55, 65, 75, 110],
            labels=['18-24', '25-34', '35-44', '45-54', '55-64', '65-74', '75+']
        )
        
        # Time-based analytics
        time_analytics = active_users.groupby(['signup_year', 'signup_month']).agg(
            user_count=('email', 'count'),
            avg_age=('age', 'mean'),
            avg_salary=('salary', 'mean'),
            median_salary=('salary', 'median')
        ).reset_index()
        
        # Demographic analytics
        demographic_analytics = active_users.groupby('age_group', observed=True).agg(
            user_count=('email', 'count'),
            avg_salary=('salary', 'mean'),
            salary_std=('salary', 'std')
        ).reset_index()
        
        # Status summary
        status_summary = df.groupby('status').agg(
            user_count=('email', 'count'),
            avg_age=('age', 'mean'),
            avg_salary=('salary', 'mean')
        ).reset_index()
        
        # Yearly growth metrics
        yearly_growth = df.groupby(df['signup_date'].dt.year).agg(
            new_users=('email', 'count')
        ).reset_index()
        yearly_growth.columns = ['year', 'new_users']
        yearly_growth['growth_pct'] = yearly_growth['new_users'].pct_change() * 100
        
        # Combined user metrics for executive dashboard
        exec_dashboard = time_analytics.groupby('signup_year').agg(
            total_users=('user_count', 'sum'),
            avg_salary=('avg_salary', 'mean')
        ).reset_index()

        # Dict with results
        dict_dfs = {
            'active_users': active_users,
            'time_analytics': time_analytics,
            'demographic_analytics': demographic_analytics,
            'status_summary': status_summary,
            'yearly_growth': yearly_growth,
            'exec_dashboard': exec_dashboard
        }
        
        # Save dataframes
        for df_name in dict_dfs.keys():
            if dict_dfs[df_name].empty:
                raise ValueError(f"Empty dataframe generated for {df_name}")
            output_path = Path(f'data/gold/{df_name}.csv')
            output_path.parent.mkdir(parents=True, exist_ok=True)
            dict_dfs[df_name].to_csv(output_path, index=False)
            logger.info(f"Successfully loaded '{df_name}' into {output_path}")

    except ValueError as e:
        logger.error(f"Validation error in gold layer: {str(e)}")
        raise
    except Exception as e:
        logger.exception("Unexpected error during gold layer creation")
        raise RuntimeError("Gold layer creation failed") from e


if __name__ == '__main__':
    file_path_parquet = os.path.join(os.path.dirname(__file__), "..", "..", "data", "silver", "silver_layer_clean.parquet")
    silver_df = pd.read_parquet(file_path_parquet)
    
    create_gold_layer(silver_df)
