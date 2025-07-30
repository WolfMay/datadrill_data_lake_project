import argparse
import os
import sys
from bronze.bronze_transformations import bronze_transformations
from silver.silver_transformations import silver_transformations
from gold.summary_report import summary_report_gold
from visualization.summary_report import summary_report_visualization

# Check if the files exist at provided path
def check_required_files(folder_path: str, required_files: list):
    missing_files = []
    for filename in required_files:
        file_path = os.path.join(folder_path, filename)
        if not os.path.exists(file_path):
            missing_files.append(filename)
    return missing_files

# Pipeline Stage Functions (Orchestrators)

# Raw to Bronze transformation
def transform_raw_to_bronze(input_path: str, output_path: str):
    print(f'\n--- Running Stage: Raw to Bronze ---')

    try:
        bronze_transformations(input_path, output_path)
        print('Raw to Bronze stage completed successfully.')
    except Exception as e:
        print(f'Error during Raw to Bronze stage: {e}')
        raise # Re-raise to stop the pipeline

def transform_bronze_to_silver(output_path):
    print(f'\n--- Running Stage: Bronze to Silver ---')

    # Check if bronze files exist before proceeding
    missing_bronze_files = check_required_files(f'{output_path}/bronze', ['employees.parquet', 'departments.parquet', 'salaries.parquet'])
    if missing_bronze_files:
        raise FileNotFoundError(f'Missing bronze files required for Silver stage: {", ".join(missing_bronze_files)}')

    try:
        silver_transformations(output_path)
        print('Bronze to Silver stage completed successfully.')
    except Exception as e:
        print(f'Error during Bronze to Silver stage: {e}')
        raise

# Reads silver data, performs final joins/aggregations, and saves the gold-level report.
def transform_silver_to_gold(output_path):
    print(f'\n--- Running Stage: Silver to Gold ---') 
    # Check if silver files exist before proceeding
    missing_silver_files = check_required_files(f'{output_path}/silver', ['employees.parquet', 'departments.parquet', 'salaries_enriched.parquet'])
    if missing_silver_files:
        raise FileNotFoundError(f'Missing silver files required for Gold stage: {", ".join(missing_silver_files)}')

    # --- INTEGRATE YOUR GOLD-LEVEL SCRIPT HERE ---
    # This script should load the silver data, perform the final aggregations
    # and create the summary_report.csv.
    # Example:
    # from your_gold_script_module import generate_gold_report
    try:
        summary_report_gold(output_path)

        print('Gold report generated successfully.')
    except Exception as e:
        print(f'Error during Silver to Gold stage: {e}')
        raise

# Orchestrates the visualization stage.
# Reads the gold-level report and generates charts.
def generate_visualizations(output_path: str):
    print(f'\n--- Running Stage: Generate Visualizations ---')
    
    # Check if gold report exists
    missing_gold_report = check_required_files(f'{output_path}/gold', ['summary_report.csv'])
    if missing_gold_report:
        raise FileNotFoundError(f'Missing gold report required for visualization stage: {", ".join(missing_gold_report)}')
    
    try:
        summary_report_visualization(output_path)
        print('Visualization stage completed successfully.')
    except Exception as e:
        print(f'Error during Visualization stage: {e}')
        raise

# Orchestrates the entire data transformation and visualization pipeline.
# Manages folder creation and sequential execution of pipeline stages.
def run_data_pipeline(input_folder: str, output_folder: str):
    print(f'--- Starting Data Pipeline ---')
    print(f'Raw Input Folder: {input_folder}')
    print(f'Output Folder: {output_folder}')

    expected_folders = ['bronze','silver','gold', 'visualization']

    # Create all necessary output directories
    print('\nEnsuring output directories exist...')
    os.makedirs(output_folder, exist_ok=True)
    print(f'Created/Ensured: {output_folder}')
    for folder in expected_folders:
        os.makedirs(os.path.join(output_folder, folder), exist_ok=True)
        print(f'Created/Ensured: {folder}')

    # Initial raw input file check
    print('\nChecking for required raw input files...')
    required_raw_files = ['employees.csv', 'departments.csv', 'salaries.csv']
    missing_raw_files = check_required_files(input_folder, required_raw_files)
    if missing_raw_files:
        print(f'Error: Missing raw files in "{input_folder}": {", ".join(missing_raw_files)}')
        sys.exit(1)
    print('All required raw input files found.')

    # Execute pipeline stages sequentially
    try:
        # Stage 1: Raw to Bronze
        transform_raw_to_bronze(input_folder, output_folder)

        # Stage 2: Bronze to Silver
        transform_bronze_to_silver(output_folder)

        # Stage 3: Silver to Gold
        transform_silver_to_gold(output_folder)

        # Stage 4: Generate Visualizations
        generate_visualizations(output_folder)

        print('\n--- Data Pipeline Completed Successfully ---')

    except Exception as e:
        print(f'\n--- Data Pipeline Failed ---')
        print(f'An error occurred: {e}')
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run a multi-stage data transformation and visualization pipeline.'
    )
    parser.add_argument(
        '--input_folder',
        type=str,
        required=True,
        help='The path to the folder containing the raw input CSV files (employees.csv, departments.csv, salaries.csv).'
    )
    parser.add_argument(
        '--output_folder',
        type=str,
        required=True,
        help='The base path to the folder where all output (bronze, silver, gold, visualizations) will be stored. ' \
        'Expects bronze/ silver/ gold/ and visualization/ folders to exist and it will create them if they do not'
    )

    args = parser.parse_args()

    # Call the main pipeline function with the parsed arguments
    run_data_pipeline(args.input_folder, args.output_folder)