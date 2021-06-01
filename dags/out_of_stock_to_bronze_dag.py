# coding=utf-8
import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, project_root)

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from handlers.out_of_stock_to_bronze_handler import out_of_stoke_to_bronze


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False
}

out_of_stock_to_bronze_dag = DAG(
    'out_of_stock_to_bronze',
    description='Export data from out_of_stock API to Bronze (HDFS)',
    schedule_interval='@daily',
    start_date=datetime(2021, 6, 1),
    default_args=default_args
)

out_of_stock = PythonOperator(
    task_id='out_of_stock_to_bronze',
    dag=out_of_stock_to_bronze_dag,
    python_callable=out_of_stoke_to_bronze
)