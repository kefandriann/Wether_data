import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

project_root = "/mnt/c/Users/kefuz/OneDrive/Desktop/Wether_data/ETL"

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from scripts.extract import extract_meteo
from scripts.merge import merge_files
from scripts.transform import transform_to_star

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 30),
}

cities = [
    "Paris", "Londres",
    "New York", "Los Angeles",
    "São Paulo", "Buenos Aires",
    "Tokyo", "Dubaï",
    "Johannesburg", "Lagos",
    "Sydney", "Melbourne"
]

with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:
    
    extract_tasks = [
        PythonOperator(
            task_id=f'extract_{city.lower().replace(" ", "_")}',
            python_callable=extract_meteo,
            op_args=[city, "{{ var.value.API_KEY }}", "{{ ds }}"],
        )
        for city in cities
    ]

    merge_task = PythonOperator(
        task_id='merge_files',
        python_callable=merge_files,
        op_args=["{{ ds }}"],
    )

    transform_task = PythonOperator(
        task_id='transform_to_star',
        python_callable=transform_to_star
    )

    extract_tasks >> merge_task >> transform_task