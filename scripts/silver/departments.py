import pandas as pd

def departments_silver(output_path):
    try:
        df = pd.read_parquet(f'{output_path}/bronze/departments.parquet')

        # No need to transform this anymore.
        # We could normalize this data by moving locations and departments into their own files and give them primary keys.
        # Also a question would be are managers employees, or do they need a separate file.
        # In the interest of time I decided against these transformations but this could be done here.

        df.to_parquet(f'{output_path}/silver/departments.parquet')
    except ValueError as e:
        print(f'Error loading data: {e}')   