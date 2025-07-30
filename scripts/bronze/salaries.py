import pandas as pd
import numpy as np
from bronze.utils import validate_post_load

def salaries_bronze(input_path, output_path):
    # Define the expected data types
    column_dtypes = {
        'employee_id': 'int64',
        'year': 'int',
        'month': 'int',
        'gross_salary': 'int'
    }

    # Define postload validation rules
    postload_validation_rules = {
        'employee_id': (int, np.integer),
        'year': int,
        'month': int,
        'gross_salary': int
    }

    try:
        df = pd.read_csv(f'{input_path}/salaries.csv', dtype=column_dtypes)

        # Validate
        validate_post_load(df, postload_validation_rules)

        df.to_parquet(f'{output_path}/bronze/salaries.parquet')
    except ValueError as e:
        print(f'Error loading data: {e}')
