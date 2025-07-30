from bronze.departments import departments_bronze
from bronze.employees import employees_bronze
from bronze.salaries import salaries_bronze

def bronze_transformations(input_path, output_path):
    departments_bronze(input_path, output_path)
    employees_bronze(input_path, output_path)
    salaries_bronze(input_path, output_path)
