from datetime import datetime, timedelta
from distutils.version import StrictVersion
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from api_to_csv import write_api_data_to_csv


default_args = {
    "owner": "Kirti",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 14),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5)

}
# setting schedule_interval="0 6 * * *" is task4 of our Assignmet
dag = DAG("Assignment", default_args=default_args, schedule_interval="0 6 * * *",
          template_searchpath=['/usr/local/airflow/sql_files'],
          catchup=False)

t1 = PythonOperator(task_id='Write_into_csv', python_callable=write_api_data_to_csv, dag=dag)
t2 = PostgresOperator(task_id='create_table', postgres_conn_id='postgres_conn', sql='create_table.sql', dag=dag)
t3 = PostgresOperator(task_id='insert_table', postgres_conn_id='postgres_conn', sql='insert_table.sql', dag=dag)
t1 >> t2 >> t3

