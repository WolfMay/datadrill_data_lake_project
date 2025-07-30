import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

def summary_report_visualization(output_path):
    try:
        summary_report_df = pd.read_csv(f'{output_path}/gold/summary_report.csv')

        # -- Chart 1: Gross Salary (Average) by Department
        fig1, ax1 = plt.subplots(figsize=(8, 5))
        ax1.bar(summary_report_df['department'], summary_report_df['gross_salary_dept_avg'])
        ax1.set_xlabel('Department')
        ax1.set_ylabel('Gross Salary (Average) (€)')
        ax1.set_title('Gross Salary (Average) by Department')
        ax1.set_ylim(0, summary_report_df['gross_salary_dept_avg'].max() * 1.1)
        plt.tight_layout()
        plt.savefig(f'{output_path}/visualization/gross_salary_dept_avg_report.png') # Changed filename for clarity
        plt.close(fig1)

        # -- Chart 2: Tenure (Average) by Department
        fig2, ax2 = plt.subplots(figsize=(8, 5))
        ax2.bar(summary_report_df['department'], summary_report_df['tenure_in_months_dept_avg'], color='green')
        ax2.set_xlabel('Department')
        ax2.set_ylabel('Tenure (Average in Months)')
        ax2.set_title('Tenure (Average) by Department')
        ax2.set_ylim(0, summary_report_df['tenure_in_months_dept_avg'].max() * 1.1)
        plt.tight_layout()
        plt.savefig(f'{output_path}/visualization/tenure_in_months_dept_avg_report.png')
        plt.close(fig2)

        # -- Chart 3: Number of Employees by Location
        fig3, ax3 = plt.subplots(figsize=(8, 5))
        ax3.bar(summary_report_df['location'], summary_report_df['number_of_employees_on_location'], color='purple')
        ax3.set_xlabel('Location')
        ax3.set_ylabel('Number of Employees')
        ax3.set_title('Number of Employees by Location')
        ax3.set_ylim(0, summary_report_df['number_of_employees_on_location'].max() * 1.1)
        ax3.yaxis.set_major_locator(mticker.MaxNLocator(integer=True)) # Set y-axis to display only whole numbers
        plt.tight_layout()
        plt.savefig(f'{output_path}/visualization/number_of_employees_report.png')
        plt.close(fig3)

    except ValueError as e:
        print(f'Error loading data: {e}')