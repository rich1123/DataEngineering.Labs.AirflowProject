from datetime import timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.example_dags.example_http_operator import default_args
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    'csv_request',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

def csv_read(path):
    all_csv = []
    for full_file in os.listdir(path):
        if full_file.endswith('.csv'):
            all_csv.append(full_file)
        return all_csv


t1 = DummyOperator()

t2 = PythonOperator(
    task_id='csv_read',
    python_callable=csv_read,
    provide_context=True,
    dag=dag)

t1 >> t2
