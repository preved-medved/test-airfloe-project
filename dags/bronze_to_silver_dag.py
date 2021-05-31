import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, project_root)

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from handlers.bronze_to_silver_handler import *

default_args = {
    "owner": "airflow",
    "email": ["airflow@airflow.com"],
    "email_ob_failure": False
}

dag = DAG(
    dag_id="bronze_to_silver",
    description="Export data from Bronze (HDFS) to Silver (HDFS)",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    default_args=default_args
)

import_orders_table = PythonOperator(
    task_id="import_orders_table",
    python_callable=import_orders_table,
    dag=dag
)

import_clients_table = PythonOperator(
    task_id="import_clients_table",
    python_callable=import_clients_table,
    dag=dag
)

import_location_areas_table = PythonOperator(
    task_id="import_location_areas_table",
    python_callable=import_location_areas_table,
    dag=dag
)

import_products_table = PythonOperator(
    task_id="import_products_table",
    python_callable=import_products_table,
    dag=dag
)

import_store_types_table = PythonOperator(
    task_id="import_store_types_table",
    python_callable=import_store_types_table,
    dag=dag
)

import_stores_table = PythonOperator(
    task_id="import_stores_table",
    python_callable=import_stores_table,
    dag=dag
)

import_departments_table = PythonOperator(
    task_id="import_departments_table",
    python_callable=import_departments_table,
    dag=dag
)

import_aisles_table = PythonOperator(
    task_id="import_aisles_table",
    python_callable=import_aisles_table,
    dag=dag
)

import_out_of_stock_table = PythonOperator(
    task_id="import_out_of_stock_table",
    python_callable=import_out_of_stock_table,
    dag=dag
)

start = DummyOperator(
    task_id="start",
    dag=dag
)

finish = DummyOperator(
    task_id="finish",
    dag=dag
)

start >> import_orders_table >> finish
start >> import_clients_table >> finish
start >> import_location_areas_table >> finish
start >> import_products_table >> finish
start >> import_store_types_table >> finish
start >> import_stores_table >> finish
start >> import_departments_table >> finish
start >> import_aisles_table >> finish
start >> import_out_of_stock_table >> finish