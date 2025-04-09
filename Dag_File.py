# Dag file

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
# Replace with a generic operator 
from LookAfter.libs.airflow.operators.CustomerScore_Opr import main_partner_score as main_partner_score

# Define default arguments for the DAG
dag_args = {
    'owner': 'data_team',  
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 24),
    'email': ['example.email@domain.com'],  # Generic email
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Initialize the DAG
dag = DAG(
    dag_id='data_processing_pipeline',  
    default_args=dag_args,
    end_date=None,
    max_active_runs=1,
    catchup=False,
    schedule_interval='0 19 * * 2',
    description='This DAG automates a data processing pipeline: it retrieves data, processes it, and updates external systems.',
    doc_md="""\
        # Data Processing Pipeline

        **Overview:**  
        This DAG automates a data processing workflow.

        **Process Steps:**  
        1. Retrieve data from a database.  
        2. Process the data.  
        3. Update results in external systems.

        **Schedule:** Once a week  
        **Owner:** Data Team
    """
)

# Define the task to process data
task_process_data = PythonOperator(
    task_id='process_data_task',
    python_callable=main_partner_score,
    dag=dag
)
