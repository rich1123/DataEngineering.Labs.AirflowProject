from datetime import timedelta
import os
from urllib.request import urlopen

import requests
import contextlib

import wget as wget
from requests import get
import csv
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


def get_data(url: str, path: str):
   """gets csv or other file from url and downloads to a particular local path"""
   wget.download(url, path)


def csv_read(path: str):
    all_csv = []
    for full_file in os.listdir(path):
        if full_file.endswith('.csv'):
            all_csv.append(full_file)
        return all_csv


def dframe():


t1 = PythonOperator(
    task_id='get data',
    python_callable=get_data,
    provide_context=True,
    dag=dag
)

t2 = PythonOperator(
    task_id='csv_read',
    python_callable=csv_read,
    provide_context=True,
    dag=dag)


t3 = PythonOperator(
    task_id='dframe',
    python_callable=dframe,
    provide_context=True,
    dag=dag)

t1 >> t2, t3




# get_data('http://pfref.com/tiny/Hd4jZ')
