import pendulum
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from big_data_operator import BigDataOperator

with DAG(
    dag_id="bigdataplugin",
    description="Big Data Plugin",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "http", "sensor"]
) as dag:
    big_data = BigDataOperator(
        task_id='big_data',
        path_to_csv_file='/opt/airflow/data/Churn.csv',
        path_to_save_file='/opt/airflow/data/Churn.parquet',
        file_type='parquet' 
    )
    
    big_data