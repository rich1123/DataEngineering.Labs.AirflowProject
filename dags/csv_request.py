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
from sqlalchemy import create_engine

dag = DAG(
    'csv_request',
    default_args=default_args,
    description='Fantasy Football ADP DAG',
    schedule_interval=timedelta(days=1),
)


# def name_data(name: str):
#     f_name = input(name)
#     return f_name

def psql_adp():
    conn = create_engine('postgresql+psycopg2://Rich2:Coder20!@localhost:5432/football')
    df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/ADP.csv', delimiter=',')
    df.to_sql(name="adp", con=conn, schema='football', if_exists='append')


def psql_rb():
    conn = create_engine('postgresql+psycopg2://Rich2:Coder20!@localhost:5432/football')
    df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/rb_data.csv', delimiter=',')
    df.to_sql(name="rb", con=conn, schema='football', if_exists='append')


def psql_wr():
    conn = create_engine('postgresql+psycopg2://Rich2:Coder20!@localhost:5432/football')
    df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/wr_data.csv', delimiter=',')
    df.to_sql(name="wr", con=conn, schema='football', if_exists='append')


def psql_qb():
    conn = create_engine('postgresql+psycopg2://Rich2:Coder20!@localhost:5432/football')
    df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/QB_data.csv', delimiter=',')
    df.to_sql(name="qb", con=conn, schema='football', if_exists='append')


def psql_def():
    conn = create_engine('postgresql+psycopg2://Rich2:Coder20!@localhost:5432/football')
    df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/def_data.csv', delimiter=',')
    df.to_sql(name="def", con=conn, schema='football', if_exists='append')


def psql_te():
    conn = create_engine('postgresql+psycopg2://Rich2:Coder20!@localhost:5432/football')
    df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/TE_data.csv', delimiter=',')
    df.to_sql(name="te", con=conn, schema='football', if_exists='append')

    # df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/adp.csv')
    # df.columns = [c.lower() for c in df.columns] #postgres doesn't like capitals or spaces
    #
    # engine = create_engine('postgresql://Rich2:Coder20!@localhost:5432/football')
    # # df.to_sql("my_table_name", engine)
    # df.to_sql("adp", engine)


# def get_data(url: str, path: str, f_name: str):
#         # print(fi)
# # def save_csv(fi: str, path: str, f_name: str):
#     file_path = os.path.join(path, f_name)
#     if not os.path.isdir(path):
#         os.mkdir(path)
#         r = requests.get(url, stream=True)
#         with open(f_name + '.csv', 'wb') as fi:
#             fi.write(r.iter_lines())
#     # file = open(file_path, "w")
#             # file.write(fi)
#             fi.close()
# """gets csv or other file from url and downloads to a particular local path"""
#
# def csv_read(path: str):
#     all_csv = []
#     for full_file in os.listdir(path):
#         if full_file.endswith('.csv'):
#             all_csv.append(full_file)
#         return all_csv
#
# moved location
#
#
# t1 = PythonOperator(
#     task_id='name_data',
#     python_callable=name_data,
#     provide_context=True,
#     dag=dag)
#
t1 = PythonOperator(
    task_id='psql_adp',
    python_callable=psql_adp,
    provide_context=True,
    dag=dag)

t2 = PythonOperator(
    task_id='psql_rb',
    python_callable=psql_rb,
    provide_context=True,
    dag=dag)

t3 = PythonOperator(
    task_id='psql_wr',
    python_callable=psql_wr,
    provide_context=True,
    dag=dag)

t4 = PythonOperator(
    task_id='psql_qb',
    python_callable=psql_qb,
    provide_context=True,
    dag=dag)

t5 = PythonOperator(
    task_id='psql_def',
    python_callable=psql_def,
    provide_context=True,
    dag=dag)

t6 = PythonOperator(
    task_id='psql_te',
    python_callable=psql_te,
    provide_context=True,
    dag=dag)




#

# t3 = PythonOperator(
#     task_id='dframe',
#     python_callable=dframe,
#     provide_context=True,
#     dag=dag)

t1.set_downstream([t2, t3, t4, t5, t6])


# name_data('test')


# get_data('http://pfref.com/pi/share/k98HH', '/Users/rmaiale/dev/airflow_home/test_dat', 'test')


# save_csv(get_data(), '/Users/rmaiale/dev/airflow_home/test_dat', 'test')


# Provided by [url=https://www.sports-reference.com/sharing.html?utm_source=direct&utm_medium=Share&utm_campaign=ShareTool]Pro-Football-Reference.com[/url]: [url=https://www.pro-football-reference.com/play-index/tiny.fcgi?id=Hd4jZ&utm_source=direct&utm_medium=Share&utm_campaign=ShareTool#results]View Original Table[/url]
