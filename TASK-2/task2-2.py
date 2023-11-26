from airflow import DAG
from datetime import datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import json

sample_data = [
    {
        "input": {
            "first_name": "sandra",
            "country": "US"
        },
        "details": {
            "credits_used": 1,
            "duration": "13ms",
            "samples": 9273,
            "country": "US",
            "first_name_sanitized": "sandra"
        },
        "result_found": True,
        "first_name": "Sandra",
        "probability": 0.98,
        "gender": "female"
    }
]

def create_table_in_db():
    create_table_sql = '''
        CREATE TABLE IF NOT EXISTS gender_name_prediction (
            input JSONB,
            details JSONB,
            result_found BOOLEAN,
            first_name VARCHAR(100),
            probability FLOAT,
            gender VARCHAR(50),
            timestamp TIMESTAMP
        );
    '''
    pg_hook = PostgresHook(postgres_conn_id='pg_conn_id')
    pg_hook.run(sql=create_table_sql)

def load_data_to_db():
    pg_hook = PostgresHook(postgres_conn_id='pg_conn_id')
    insert_query = '''
        INSERT INTO gender_name_prediction (input, details, result_found, first_name, probability, gender, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    '''
    for data in sample_data:
        timestamp = datetime.utcnow()  # Assuming the current time as timestamp for each record
        pg_hook.run(insert_query, parameters=(
            json.dumps(data['input']),
            json.dumps(data['details']),
            data['result_found'],
            data['first_name'],
            data['probability'],
            data['gender'],
            timestamp
        ))

with DAG(
    dag_id='alterra_task2_db',
    schedule_interval=None,
    start_date=datetime(2022, 10, 21),
    catchup=False
) as dag:

    create_table_task = PythonOperator(
        task_id='create_table_in_db',
        python_callable=create_table_in_db,
        dag=dag
    )

    load_data_task = PythonOperator(
        task_id='load_data_to_db',
        python_callable=load_data_to_db,
        dag=dag
    )

    create_table_task >> load_data_task
