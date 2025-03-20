import pandas as pd
import numpy as np
from faker import Faker
from random import choice, choices, randint, random

fake = Faker()

def generate_messy_data(num_records=100, num_duplicates=5):
    data = {
        'name': [fake.name() if random() > 0.1 else None for _ in range(num_records)],
        'email': [fake.email() for _ in range(num_records)],
        'phone': [f"({randint(100,999)}) {randint(100,999)}-{randint(1000,9999)}" if random() > 0.2 
                 else str(randint(1000000000, 9999999999)) for _ in range(num_records)],
        'age': [
            choices(list(range(151)), weights=[10 if 18 <= age <= 65 else 1 for age in range(151)], k=1)[0] 
            if random() > 0.1 else 'unknown' for _ in range(num_records)
        ],
        'salary': [randint(20000, 200000) if random() > 0.9 else randint(-5000, 19999) for _ in range(num_records)],
        'address': [fake.address().replace("\n", " ") if random() > 0.3 else "   " for _ in range(num_records)],
        'signup_date': [fake.date_this_decade().isoformat() if random() > 0.15 
                       else f"{randint(1,13)}/{randint(1,32)}/{randint(2000,2023)}" for _ in range(num_records)],
        'status': [choice(['active', 'ACTIVE', 'inactive', 'INACTIVE', 'pending', '']) for _ in range(num_records)]
    }
    df = pd.DataFrame(data)
    df = pd.concat([df, df.sample(num_duplicates)], ignore_index=True)
    return df

# Generate messy bronze layer data
raw_df = generate_messy_data(1000, 50)
raw_df.to_csv('data/raw/bronze_layer_raw.csv', index=False)

