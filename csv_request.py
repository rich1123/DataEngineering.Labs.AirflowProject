from datetime import timedelta
import os
from urllib.request import urlopen
# import wget as wget
import requests
import contextlib

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


# def name_data(name: str):
#     f_name = input(name)
#     return f_name

def get_data(url: str, path: str, f_name: str):
        # print(fi)
# def save_csv(fi: str, path: str, f_name: str):
    file_path = os.path.join(path, f_name)
    if not os.path.isdir(path):
        os.mkdir(path)
        r = requests.get(url)
        with open(f_name + '.csv', 'wb') as fi:
            fi.write(r.content)
    # file = open(file_path, "w")
            # file.write(fi)
            fi.close()
# """gets csv or other file from url and downloads to a particular local path"""
#
# def csv_read(path: str):
#     all_csv = []
#     for full_file in os.listdir(path):
#         if full_file.endswith('.csv'):
#             all_csv.append(full_file)
#         return all_csv
#
#
#
#
#     t1 = PythonOperator(
#     task_id='get data',
#     python_callable=get_data,
#     provide_context=True,
#     dag=dag)
#
# t2 = PythonOperator(
#     task_id='csv_read',
#     python_callable=csv_read,
#     provide_context=True,
#     dag=dag)
#
# t3 = PythonOperator(
#     task_id='dframe',
#     python_callable=dframe,
#     provide_context=True,
#     dag=dag)

# t1 >> t2, t3


# name_data('test')


get_data('http://pfref.com/tiny/Hd4jZ', '/Users/rmaiale/dev/airflow_home/test_dat', 'test')


# save_csv(get_data(), '/Users/rmaiale/dev/airflow_home/test_dat', 'test')