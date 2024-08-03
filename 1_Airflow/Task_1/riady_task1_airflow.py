from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator

# Number 1 Create DAG that run in every 5 hours.
dag=DAG(
    'riady_airflow_task1', 
    description='Riady Airflow Task 1',
    schedule_interval='0 */5 * * *',
    start_date=datetime(2023, 10, 21), 
    catchup=False
)
start = EmptyOperator(
    task_id='start',
    dag=dag,
)
    # ti = task instance
    # Number 2 Suppose we define a new task that push a variable to xcom.
def push_var_to_xcom_new(ti=None):
    ti.xcom_push(key='departement', value='Bussiness Development')
    ti.xcom_push(key='departement1', value='Human Resources')
    ti.xcom_push(key='departement2', value='IT Support')

    #Number 3 How to pull multiple values at once?
def pull_multiple_values_new(ti=None):
    departement = ti.xcom_pull(task_ids='push_var_department', key='departement')
    departement1 = ti.xcom_pull(task_ids='push_var_department', key='departement1')
    departement2 = ti.xcom_pull(task_ids='push_var_department', key='departement2')

    print(f'print departement variable from xcom: {departement}, {departement1}, {departement2}')

push_var_to_xcom_new = PythonOperator(
    task_id = 'push_var_to_xcom_new',
    python_callable = push_var_to_xcom_new
)

pull_multiple_values_new = PythonOperator(
    task_id = 'pull_multiple_values_new',
    python_callable = pull_multiple_values_new
)

start >> push_var_to_xcom_new >> pull_multiple_values_new