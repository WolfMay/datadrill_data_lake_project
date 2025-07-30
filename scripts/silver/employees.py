import pandas as pd
import datetime

def employees_silver(output_path):
    try:
        employees_df = pd.read_parquet(f'{output_path}/bronze/employees.parquet')

        # Coerce start_date to datetime
        employees_df['start_date_validated'] = pd.to_datetime(employees_df['start_date'], errors='coerce')
        invalid_date_rows = employees_df[employees_df['start_date_validated'].isnull() & employees_df['start_date'].notnull()]

        if not invalid_date_rows.empty:
            print("\nInvalid date values found in 'start_date' column:")
            print(invalid_date_rows[['start_date', 'start_date_validated']])

        employees_df['start_date'] = employees_df['start_date_validated']
        employees_df = employees_df.drop(columns=['start_date_validated'])

        # Calculate tenure in months
        employees_df['tenure_in_months'] = (datetime.datetime.now().year - employees_df['start_date'].dt.year) * 12 + (datetime.datetime.now().month - employees_df['start_date'].dt.month)
        
        employees_df.to_parquet(f'{output_path}/silver/employees.parquet')
    except ValueError as e:
        print(f'Error loading data: {e}')