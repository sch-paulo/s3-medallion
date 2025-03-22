import pandas as pd
from faker import Faker
from random import choice, choices, randint, random

fake = Faker()

def generate_raw_data(num_records, num_duplicates):
    """
    Generate synthetic data with intentional quality issues to simulate bronze layer data.
    
    Parameters:
    num_records (int): Number of unique records to generate
    num_duplicates (int): Number of duplicate records to add to the dataset
    
    Returns:
    DataFrame: Pandas DataFrame containing synthetic data with various quality issues including 
    null values, inconsistent formatting, and duplicate records
   """
    data = {
        'name': [fake.name() if random() > 0.03 else None for _ in range(num_records)],
        'email': [fake.email() for _ in range(num_records)],
        'phone': [f"({randint(100,999)}) {randint(100,999)}-{randint(1000,9999)}" if random() > 0.2 
                 else str(randint(1000000000, 9999999999)) for _ in range(num_records)],
        'age': [
            choices(list(range(151)), weights=[15 if 18 <= age <= 65 else 1 for age in range(151)], k=1)[0] 
            if random() > 0.05 else 'unknown' for _ in range(num_records)
        ],
        'salary': [randint(2000, 50000) if random() > 0.92 else randint(-5000, 19999) for _ in range(num_records)],
        'address': [fake.address().replace("\n", " ") if random() > 0.2 else "   " for _ in range(num_records)],
        'signup_date': [fake.date_between_dates(date_start='-5y', date_end='now').isoformat() if random() > 0.1 
                       else f"{randint(1,31)}/{randint(1,12)}/{randint(2019,2025)}" for _ in range(num_records)],
        'status': choices(['active', 'ACTIVE', 'inactive', 'INACTIVE', 'pending', ''], weights=[8, 8, 4, 4, 1, 0.5], k=num_records)
    }   
    df = pd.DataFrame(data)
    df = pd.concat([df, df.sample(num_duplicates)], ignore_index=True)
    return df

if __name__ == '__main__':   
    raw_df = generate_raw_data(10000, 120)
    raw_df.to_csv('data/raw/bronze_layer_raw.csv', index=False)


