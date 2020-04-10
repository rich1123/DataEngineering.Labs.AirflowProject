from datetime import timedelta

import pandas as pd
import pdfkit
import sqlalchemy
from airflow import DAG
# from airflow.example_dags.example_http_operator import default_args
# from airflow.operators.papermill_operator import PapermillOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.utils.dates import days_ago
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import seaborn as sns

# from slackclient import SlackClient


# SLACK_CONN_ID = 'slack'

# def task_fail_slack_alert(context):
#     """
#     Sends message to a slack channel.
#     If you want to send it to a "user" -> use "@user",
#         if "public channel" -> use "#channel",
#         if "private channel" -> use "channel"
#     """
#     slack_channel = BaseHook.get_connection(SLACK_CONN_ID).login
#     slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
#     failed_alert = SlackAPIPostOperator(
#         task_id='slack_failed',
#         channel=slack_channel,
#         token=slack_token,
#         text="""
#             :red_circle: Task Failed.
#             *Task*: {task}
#             *Dag*: {dag}
#             *Execution Time*: {exec_date}
#             *Log Url*: {log_url}
#             """.format(
#             task=context.get('task_instance').task_id,
#             dag=context.get('task_instance').dag_id,
#             ti=context.get('task_instance'),
#             exec_date=context.get('execution_date'),
#             log_url=context.get('task_instance').log_url,
#         )
#     )
#     return failed_alert.execute(context=context)

default_args = {
    'owner': 'Rich maiale',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['richm1123@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'csv_request',
    default_args=default_args,
    description='Fantasy Football ADP DAG',
    schedule_interval=timedelta(days=1),
)


# XCom.set(
#     key='f_name',
#     value='rb',
#     task_id=t1.task_id,
#     dag_id=t1.dag_id,
#     execution_date=PythonOperator)
# def name_data(name: str):
#     f_name = input(name)
#     return f_name

# def df_update():
#     adp_temp = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/ADP.csv')
#     rb_temp = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/rb_data.csv', dtype={'Year': object})
#     wr_temp = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/wr_data.csv')
#     qb_temp = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/QB_data.csv')
#     te_temp = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/TE_data.csv')
#     def_temp = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/def_data.csv')
#     rb_adp = pd.concat([rb_temp, adp_temp], keys=['Pick'], axis=1)
#     wr_adp = pd.concat([wr_temp, adp_temp], keys=['Pick'], axis=1)
#     qb_adp = pd.concat([qb_temp, adp_temp], keys=['Pick'], axis=1)
#     te_adp = pd.concat([te_temp, adp_temp], keys=['Pick'], axis=1)
#     def_adp = pd.concat([def_temp, adp_temp], keys=['Pick'], axis=1)
#     rb_adp.to_csv(r'rb_data.csv', index=False)
#     wr_adp.to_csv(r'wr_data.csv', index=False)
#     qb_adp.to_csv(r'QB_data.csv', index=False)
#     te_adp.to_csv(r'TE_data.csv', index=False)
#     def_adp.to_csv(r'def_data.csv', index=False)


# print(wr_adp, qb_adp, te_adp, def_adp)

#     # return adp_temp

# def psql_adp():
#     conn = sqlalchemy.create_engine('postgresql+psycopg2://Rich2:Coder20!@localhost:5432/ff_fb')
#     df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/ADP.csv', delimiter=',')
#     # conn.connect() as conn, conn.begin():
#     df.to_sql(name="adp", con=conn, schema='fantasy_data', if_exists='replace')
#
#
# def psql_rb():
#     conn = sqlalchemy.create_engine('postgresql+psycopg2://Rich2:Coder20!@localhost:5432/ff_fb')
#     df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/rb_data.csv', delimiter=',')
#     #     with conn.connect() as conn, conn.begin():
#     df.to_sql(name="rb", con=conn, schema='fantasy_data', if_exists='replace')
#
#
# def psql_wr():
#     conn = sqlalchemy.create_engine('postgresql+psycopg2://Rich2:Coder20!@localhost:5432/ff_fb')
#     df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/wr_data.csv', delimiter=',')
#     # with conn.connect() as conn, conn.begin():
#     df.to_sql(name="wr", con=conn, schema='fantasy_data', if_exists='replace')
#
#
# def psql_qb():
#     conn = sqlalchemy.create_engine('postgresql+psycopg2://Rich2:Coder20!@localhost:5432/ff_fb')
#     df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/QB_data.csv', delimiter=',')
#     # with conn.connect() as conn, conn.begin():
#     df.to_sql(name="qb", con=conn, schema='fantasy_data', if_exists='replace')
#
#
# def psql_te():
#     conn = sqlalchemy.create_engine('postgresql+psycopg2://Rich2:Coder20!@localhost:5432/ff_fb')
#     df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/TE_data.csv', delimiter=',')
#     # with conn.connect() as conn, conn.begin():
#     df.to_sql(name="te", con=conn, schema='fantasy_data', if_exists='replace')
#
#
# def psql_def():
#     conn = sqlalchemy.create_engine('postgresql+psycopg2://Rich2:Coder20!@localhost:5432/ff_fb')
#     df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/def_data.csv', delimiter=',')
#     # with conn.connect() as conn, conn.begin():
#     df.to_sql(name="def", con=conn, schema='fantasy_data', if_exists='replace')


# df = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/adp.csv')
# df.columns = [c.lower() for c in df.columns] #postgres doesn't like capitals or spaces
#
# engine = create_engine('postgresql://Rich2:Coder20!@localhost:5432/ff_fb')
# # df.to_sql("my_table_name", engine)
# df.to_sql("adp", engine)

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
#

def adp_tables():
    con = sqlalchemy.create_engine('postgresql://Rich2:Coder20!@localhost:5432/ff_fb')
    rb = pd.read_sql_table('rb', con)
    AP = rb.loc[rb['Player'] == 'Adrian Peterson']
    AP.describe()
    AP_snapshot = {plt.figure(figsize=(8, 6), dpi=240),
                   plt.xlim(2010, 2019),
                   plt.ylim(0, 10),
                   plt.xlabel('Year'), plt.ylabel('Pick'),
                   plt.title('Year to Pick'),
                   plt.scatter(x=AP['Year'], y=AP['Pick']),
                   plt.show(),
                   }
    plt.savefig('AP_snapshot.pdf')
    plt.savefig('AP_snapshot.png')
    AP_yp = AP.plot(x='Year', y='Pick')
    plt.savefig('AP_yp.pdf')
    plt.savefig('AP_yp.png')
    AP_year_yds = AP.plot(x='Year', y='Rush Yds')
    plt.savefig('AP_year_yds.pdf')
    plt.savefig('AP_year_yds.png')
    #
    # rb.to_csv()
    # # rb.to_string()
    # pdfkit.from_file('rb.csv', 'rb.pdf')


data_jawn = PythonOperator(
    task_id='pdfs',
    python_callable=adp_tables,
    provide_context=False,
    dag=dag,
)

# df = PythonOperator(
#     task_id='dframes',
#     python_callable=df_update,
#     provide_context=False,
#     dag=dag,
# )

# t1 = PythonOperator(
#     task_id='psql_adp',
#     python_callable=psql_adp,
#     provide_context=False,
#     # do_xcom_push=True,
#     dag=dag,
# )
#
# t2 = PythonOperator(
#     task_id='psql_rb',
#     python_callable=psql_rb,
#     provide_context=False,
#     dag=dag,
# )
#
# t3 = PythonOperator(
#     task_id='psql_wr',
#     python_callable=psql_wr,
#     provide_context=False,
#     dag=dag,
# )
#
# t4 = PythonOperator(
#     task_id='psql_qb',
#     python_callable=psql_qb,
#     provide_context=False,
#     dag=dag,
# )
#
# t5 = PythonOperator(
#     task_id='psql_def',
#     python_callable=psql_def,
#     provide_context=False,
#     dag=dag,
# )
#
# t6 = PythonOperator(
#     task_id='psql_te',
#     python_callable=psql_te,
#     provide_context=False,
#     dag=dag,
# )

# failure = SlackAPIPostOperator(
#     task_id='failure',
#     token='slack_token',
#     text='Hello World !',
#     channel='@richmaiale',  # Replace with your Slack username
#     username='airflow',
# )

# run_this = PapermillOperator(
#     task_id="run_notebook",
#     input_nb="/Users/Rich2/dev/DataEngineering.Labs.AirflowProject/dags/football.ipynb",
#     start_date=datetime(2018, 1, 1),
#     output_nb="/Users/Rich2/dev/DataEngineering.Labs.AirflowProject/dags/out-{{ execution_date }}.ipynb",
#     parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"}
# )

#

# run_noteboook = PaperMillOperator(
#     task_id='dframe',
#     python_callable=dframe,
#     provide_context=True,
#     dag=dag)

# t1.set_downstream([t2, t3, t4, t5, t6, run_this])
#
# t1.set_downstream([t2, t3, t4, t5, t6])
# t1.set_upstream(run_this)

# t1
# t2
# t3
# t4
# t5
# t6

# t1

# df

# save_csv(get_data(), '/Users/rmaiale/dev/airflow_home/test_dat', 'test')


# Provided by [url=https://www.sports-reference.com/sharing.html?utm_source=direct&utm_medium=Share&utm_campaign=ShareTool]Pro-Football-Reference.com[/url]: [url=https://www.pro-football-reference.com/play-index/tiny.fcgi?id=Hd4jZ&utm_source=direct&utm_medium=Share&utm_campaign=ShareTool#results]View Original Table[/url]

# psql_adp()


# if __name__ == "__main__":
#     psql_adp()
#     psql_def()
#     psql_qb()
#     psql_rb()
#     psql_te()
#     psql_wr()

# xoxp-72228144001-974172786532-1072787575472-2d5e07b8cc2e580675433738dd30437b
adp_tables()
