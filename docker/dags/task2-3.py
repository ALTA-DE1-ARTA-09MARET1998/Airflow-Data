from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import json

prediction_results = [
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

def load_prediction_results_to_db():
    pg_hook = PostgresHook(postgres_conn_id='pg_conn_id')
    insert_query = '''
        INSERT INTO gender_name_prediction (input, details, result_found, first_name, probability, gender, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    '''
    for data in prediction_results:
        timestamp = datetime.utcnow()
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
    dag_id='prediction_result_arta',
    schedule_interval=None,
    start_date=datetime(2023, 11, 26),
    catchup=False
) as dag:

    load_results_task = PythonOperator(
        task_id='load_prediction_results_to_db',
        python_callable=load_prediction_results_to_db,
        dag=dag
    )

    load_results_task
