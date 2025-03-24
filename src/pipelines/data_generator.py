import pandas as pd
from pathlib import Path
from faker import Faker
from random import choices, randint, random
import logging

fake = Faker()
logger = logging.getLogger(__name__)

def generate_raw_data(num_records: int, num_duplicates: int, file_name: str = 'bronze_layer_raw') -> None:
    '''
    Generate synthetic data with intentional quality issues and save the 
    result as a bronze layer CSV file.
    
    Parameters:
    num_records (int): Number of unique records to generate
    num_duplicates (int): Number of duplicate records to add to the dataset
    file_name (str, optional): Name for the output CSV file. Defaults to 'bronze_layer_raw'
    
    Returns:
    None: The function saves the raw data to a CSV file but doesn't return a value
    '''
    try:
        if not isinstance(num_records, int) or num_records <= 0:
            raise ValueError(f"Invalid 'num_records': {num_records}. Must be positive integer")
            
        logger.info(f"Generating {num_records} records with {num_duplicates} duplicates")

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

        output_path = Path(f'data/bronze/{file_name}.csv')
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(output_path, index=False)
        logger.info(f"Successfully generated data to {output_path}")

    except (ValueError, PermissionError) as e:
        logger.error(f"Data generation failed: {str(e)}")
        raise
    except Exception as e:
        logger.exception("Unexpected error during data generation")
        raise RuntimeError("Data generation failed") from e


if __name__ == '__main__':   
    generate_raw_data(10000, 120)
