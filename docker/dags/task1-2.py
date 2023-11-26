# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator

# dag = DAG(
#     'alterra_hello_world_with_operator_arta',
#     description='Push and Pull variable DAG',
#     schedule_interval=None,
#     start_date=datetime(2023, 10, 25), 
#     catchup=False
# )

# def push_task_a(ti=None):
#     ti.xcom_push(key='student', value='Arta from Data Engineering 101')
    
# def get_task_a(ti=None):
#     student = ti.xcom_pull(task_ids='push_task_a', key='student')
#     print(f'print alterra variable from xcom: {student}')

# push_task_a_alterra = PythonOperator(
#     task_id = 'push_task_a',
#     python_callable = push_task_a
# )

# get_task_a_alterra = PythonOperator(
#     task_id = 'get_task_a',
#     python_callable = get_task_a
# )

# push_task_a_alterra >> get_task_a_alterra

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

with DAG(
    'Alterra_XCom_Native_Arta', 
    description='Print xcom Example DAG',
    schedule_interval=None,
    start_date=datetime(2023, 1, 28), 
    catchup=False
) as dag:
    
    def push_var_from_task_c(ti=None):
        ti.xcom_push(key='isarter', value='Arta Data Engineering 101')
    
    def get_var_from_task_c(ti=None):
        isarter = ti.xcom_pull(task_ids='push_var_from_task_c', key='isarter')
        print(f'print alterra variable from xcom: {isarter}')

    push_var_from_task_c_task = PythonOperator(
        task_id = 'push_var_from_task_c',
        python_callable = push_var_from_task_c
    )

    get_var_from_task_c_task = PythonOperator(
        task_id = 'get_var_from_task_c',
        python_callable = get_var_from_task_c
    )

    push_var_from_task_c_task >> get_var_from_task_c_task