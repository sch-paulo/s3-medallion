import os
import pandas as pd

def create_gold_layer(silver_df):
    """
    Transform silver layer data into gold layer analytics-ready data.
    
    Parameters:
    silver_df (DataFrame): Cleaned silver layer data
    
    Returns:
    tuple: Multiple DataFrames for different analytical purposes
    """
    # Make a copy to avoid modifying the input
    df = silver_df.copy()
    
    # Convert signup_date to datetime for proper analysis
    df['signup_date'] = pd.to_datetime(df['signup_date'])
    
    # 1. Active Users Analysis (building on your sketch)
    active_users = df[df['status'] == 'Active'].copy()
    active_users['signup_year'] = active_users['signup_date'].dt.year
    active_users['signup_month'] = active_users['signup_date'].dt.month
    active_users['age_group'] = pd.cut(
        active_users['age'], 
        bins=[0, 18, 25, 35, 45, 55, 65, 110],
        labels=['0-18', '19-25', '26-35', '36-45', '46-55', '56-65', '65+']
    )
    
    # 2. Time-based analytics
    time_analytics = active_users.groupby(['signup_year', 'signup_month']).agg(
        user_count=('email', 'count'),
        avg_age=('age', 'mean'),
        avg_salary=('salary', 'mean'),
        median_salary=('salary', 'median')
    ).reset_index()
    
    # 3. Demographic analytics
    demographic_analytics = active_users.groupby('age_group', observed=True).agg(
        user_count=('email', 'count'),
        avg_salary=('salary', 'mean'),
        salary_std=('salary', 'std')
    ).reset_index()
    
    # 4. Status summary
    status_summary = df.groupby('status').agg(
        user_count=('email', 'count'),
        avg_age=('age', 'mean'),
        avg_salary=('salary', 'mean')
    ).reset_index()
    
    # 5. Yearly growth metrics
    yearly_growth = df.groupby(df['signup_date'].dt.year).agg(
        new_users=('email', 'count')
    ).reset_index()
    yearly_growth.columns = ['year', 'new_users']
    yearly_growth['growth_pct'] = yearly_growth['new_users'].pct_change() * 100
    
    # 6. Combined user metrics for executive dashboard
    exec_dashboard = time_analytics.groupby('signup_year').agg(
        total_users=('user_count', 'sum'),
        avg_salary=('avg_salary', 'mean')
    ).reset_index()
    
    return {
        'active_users': active_users,
        'time_analytics': time_analytics,
        'demographic_analytics': demographic_analytics,
        'status_summary': status_summary,
        'yearly_growth': yearly_growth,
        'exec_dashboard': exec_dashboard
    }


if __name__ == '__main__':
    file_path_parquet = os.path.join(os.path.dirname(__file__), "..", "..", "data", "processed", "silver_layer_clean.parquet")
    silver_df = pd.read_parquet(file_path_parquet)
    
    gold_data = create_gold_layer(silver_df)
    print(gold_data['exec_dashboard'])
    print(gold_data['demographic_analytics'])