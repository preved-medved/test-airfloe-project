import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, project_root)

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from handlers.pg_to_bronze_handler import pg_to_bronze

default_args = {
    "owner": "airflow",
    "email": ["airflow@airflow.com"],
    "email_ob_failure": False
}


def return_tables():
    return ['aisles',
            'clients',
            'departments',
            'location_areas',
            'orders',
            'products',
            'store_types',
            'stores',
            ]


def load_task(table):
    return PythonOperator(
        task_id="load_table_" + table,
        python_callable=pg_to_bronze,
        op_kwargs={"table_name": table},
        dag=dag
    )


dag = DAG(
    dag_id="pg_to_bronze",
    description="Export data from PG to Bronze (HDFS)",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    default_args=default_args
)

start = DummyOperator(
    task_id="start",
    dag=dag
)

finish = DummyOperator(
    task_id="finish",
    dag=dag
)

for table_name in return_tables():
    start >> load_task(table_name) >> finish
