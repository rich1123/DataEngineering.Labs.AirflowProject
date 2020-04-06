from datetime import timedelta
import os
from urllib.request import urlopen

import requests
import contextlib

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

def mysql():
    conn = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/football')
    df = pd.read_csv('/Users/rmaiale/dev/airflow_home/', delimiter=',') 
    df.to_sql(name='nyc_housing', con=conn, schema='airflow_project', if_exists='replace')



def name_data(name: str):
    f_name = input(name)
    return f_name

def get_data(url: str, path: str, f_name: str):
        # print(fi)
# def save_csv(fi: str, path: str, f_name: str):
    file_path = os.path.join(path, f_name)
    if not os.path.isdir(path):
        os.mkdir(path)
        r = requests.get(url, stream=True)
        with open(f_name + '.csv', 'wb') as fi:
            fi.write(r.iter_lines())
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
#moved location
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


name_data('test')


get_data('http://pfref.com/pi/share/k98HH', '/Users/rmaiale/dev/airflow_home/test_dat', 'test')


# save_csv(get_data(), '/Users/rmaiale/dev/airflow_home/test_dat', 'test')


# Provided by [url=https://www.sports-reference.com/sharing.html?utm_source=direct&utm_medium=Share&utm_campaign=ShareTool]Pro-Football-Reference.com[/url]: [url=https://www.pro-football-reference.com/play-index/tiny.fcgi?id=Hd4jZ&utm_source=direct&utm_medium=Share&utm_campaign=ShareTool#results]View Original Table[/url]
