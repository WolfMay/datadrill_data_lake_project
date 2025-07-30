import pandas as pd

def salaries_enriched_silver(output_path):
    try:
        employees_df = pd.read_parquet(f'{output_path}/silver/employees.parquet')
        salaries_df = pd.read_parquet(f'{output_path}/bronze/salaries.parquet')
        departments_df = pd.read_parquet(f'{output_path}/bronze/departments.parquet')

        # Merge Employees
        salaries_enriched_df = pd.merge(salaries_df, employees_df, on='employee_id', how='left')

        # Merge Departments
        salaries_enriched_df = pd.merge(salaries_enriched_df, departments_df, on='department', how='left')

        salaries_enriched_df.to_parquet(f'{output_path}/silver/salaries_enriched.parquet')
    except ValueError as e:
        print(f'Error loading data: {e}')