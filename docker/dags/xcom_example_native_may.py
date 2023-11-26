from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

with DAG(
    'Orin_Alterra_XCom_Native', 
    description='Print xcom Example DAG',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1), 
    catchup=False
) as dag:
    # ti = task instance
    def push_var_from_task_b(ti=None):
        ti.xcom_push(key='alterra', value='Orin Data Engineering 101')
    
    def get_var_from_task_b(ti=None):
        alterra = ti.xcom_pull(task_ids='push_var_from_task_b', key='alterra')
        print(f'print alterra variable from xcom: {alterra}')

    push_var_from_task_b_task = PythonOperator(
        task_id = 'push_var_from_task_b',
        python_callable = push_var_from_task_b
    )

    get_var_from_task_b_task = PythonOperator(
        task_id = 'get_var_from_task_b',
        python_callable = get_var_from_task_b
    )

    push_var_from_task_b_task >> get_var_from_task_b_task