import pandas as pd
import numpy as np
from bronze.utils import validate_post_load

def employees_bronze(input_path, output_path):
    # Define the expected data types
    column_dtypes = {
        'employee_id': 'int64',
        'first_name': 'object',
        'last_name': 'object',
        'position': 'object',
        'start_date': 'object',
        'department': 'object'
    }

    # Define postload validation rules
    postload_validation_rules = {
        'employee_id': (int, np.integer),
        'first_name': str,
        'last_name': str,
        'position': str,
        'start_date': str,
        'department': str
    }

    try:
        df = pd.read_csv(f'{input_path}/employees.csv', dtype=column_dtypes)

        # Validate
        validate_post_load(df, postload_validation_rules)

        df.to_parquet(f'{output_path}/bronze/employees.parquet')
    except ValueError as e:
        print(f'Error loading data: {e}')