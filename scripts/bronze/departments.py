import pandas as pd
from bronze.utils import validate_post_load

def departments_bronze(input_path, output_path):
    # Define the expected data types
    column_dtypes = {
        'department': object,
        'location': object,
        'manager': object
    }

    # Define postload validation rules
    postload_validation_rules = {
        'department': str,
        'location': str,
        'manager': str
    }

    try:
        df = pd.read_csv(f'{input_path}/departments.csv', dtype=column_dtypes)

        # Validate
        validate_post_load(df, postload_validation_rules)

        df.to_parquet(f'{output_path}/bronze/departments.parquet')
    except ValueError as e:
        print(f'Error loading data: {e}')   