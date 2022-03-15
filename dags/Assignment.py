from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from task1 import csv_creation
from task2 import table_creation
from task3 import populate_table

default_args = {
    "owner": "Kartik",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 15),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG("Assignment", default_args=default_args, schedule_interval="0 6 * * *")

t1 = PythonOperator(task_id='csv_creation_t', python_callable=csv_creation, dag=dag)

t2 = PythonOperator(task_id="table_creation_t", python_callable=table_creation, dag=dag)

t3 = PythonOperator(task_id="populate_table_t", python_callable=populate_table, dag=dag)

t1 >> t2 >> t3
