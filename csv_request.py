from airflow.operators.python_operator import PythonOperator
import pandas as pd



dag = DAG(
    'csv_request',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

def csv_read(path):
    all_csv = []
    for full_file in os.listdir(path):
        if full_file.endswith('.csv')::
                all_csv.append(fi)
        return all_csv


t1 = PythonOperator(
        task_id='download_file',
        python_callable=download_file_from_ftp,
        provide_context=True,
       	},
  dag=dag)

t1 = DummyOperator()


t1 >> t2
