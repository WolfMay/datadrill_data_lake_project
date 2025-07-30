import pandas as pd

def summary_report_gold(output_path):
    try:
        salaries_enriched_df = pd.read_parquet(f'{output_path}/silver/salaries_enriched.parquet')

        # Calculate employees per location separately
        employees_per_location_df = salaries_enriched_df.groupby('location')['employee_id'].nunique().reset_index()
        employees_per_location_df.rename(columns={'employee_id': 'number_of_employees_on_location'}, inplace=True)

        # Calculate department metrics

        # Filter for the most recent salary per employee
        most_recent_salaries_df = salaries_enriched_df.sort_values(
            ['employee_id', 'year', 'month'],
            ascending=[True, False, False]
        ).drop_duplicates(subset='employee_id', keep='first')

        # Group by department for average gross salary and average tenure
        department_metrics_df = most_recent_salaries_df.groupby('department').agg(
            gross_salary_dept_avg=('gross_salary', 'mean'),
            tenure_in_months_dept_avg=('tenure_in_months', 'mean')
        ).reset_index()


        # Add location column to department metrics
        department_metrics_with_location_df = pd.merge(
            department_metrics_df,
            salaries_enriched_df[['department', 'location']], # Select only necessary columns from departments_df
            on='department',
            how='left'
        )

        # Merge on location to combine departmental averages with employee counts
        summary_report_df = pd.merge(
            department_metrics_with_location_df,
            employees_per_location_df,
            on='location',
            how='left'
        )

        # Add the boolean column for the longest average tenure
        max_tenure = summary_report_df['tenure_in_months_dept_avg'].max()
        summary_report_df['is_longest_tenure_dept'] = summary_report_df['tenure_in_months_dept_avg'] == max_tenure

        summary_report_df.to_csv(f'{output_path}/gold/summary_report.csv', index=False)
    except ValueError as e:
        print(f'Error loading data: {e}')