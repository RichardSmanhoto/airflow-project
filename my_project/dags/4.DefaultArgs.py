import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "depends_on_past": False,
    "email": ["richard.11smanhoto@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id="defaultargs_dag",
    description="DAG TESTE DEFAULT ARGS",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Default Dags", "Sozinho"],
    default_args=default_args
) as dag:
    task1 = BashOperator(task_id="task_1", bash_command="exit 1")
    task2 = BashOperator(task_id="task_2", bash_command="sleep 5")
    task3 = BashOperator(task_id="task_3", bash_command="sleep 5")
    

    task1 >> task2 >> task3