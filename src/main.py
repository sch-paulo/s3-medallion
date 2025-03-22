from src.data_generator import generate_raw_data
from src.processor import clean_silver_layer
from src.aggregators import create_gold_layer

# Generate and clean data
bronze_df = generate_raw_data(5000, 100)
silver_df = clean_silver_layer(bronze_df)
gold_data = create_gold_layer(silver_df)

# Access specific analytics
print(f'Lenght silver df: {len(silver_df)}', '\n')
print(gold_data['active_users'], '\n')
print(gold_data['time_analytics'], '\n')
print(gold_data['status_summary'], '\n')
print(gold_data['yearly_growth'], '\n')
print(gold_data['exec_dashboard'], '\n')
print(gold_data['demographic_analytics'], '\n')