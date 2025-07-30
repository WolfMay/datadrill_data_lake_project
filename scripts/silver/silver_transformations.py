from silver.departments import departments_silver
from silver.employees import employees_silver
from silver.salaries_enriched import salaries_enriched_silver

def silver_transformations(output_path):
    employees_silver(output_path)
    departments_silver(output_path)
    salaries_enriched_silver(output_path)
