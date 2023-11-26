from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


# def print_coba():
#     return 'Ini task 1 Airflow!'

def push_task1(ti=None):
    ti.xcom_push(key='eclipse', value='the twilight saga')

def get_task1(ti=None):
    eclipse = ti.xcom_pull(task_ids='push_task1', key='eclipse')
    print(f'print eclipse variable from xcom: {eclipse}')
    
dag = DAG(
        'alterra_hello_world_arta', 
        description='Hello Arter, ini tugas 1 Airflow DAG',
        schedule_interval='0 */5 * * *',
        start_date = datetime(2023, 11, 25),
        catchup = False
    )

task1 = PythonOperator(
    task_id='taskairflow_1', 
    python_callable=get_task1, 
    dag=dag
)

task1