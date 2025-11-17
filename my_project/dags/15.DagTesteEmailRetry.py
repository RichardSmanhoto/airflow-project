import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

default_args = {
    "depends_on_past": False,
    "email": ["richard.11smanhoto@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    'retries': 1
}

with DAG(
    dag_id="teste_email_retry",
    description="teste email",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "Sozinho"],
    default_args=default_args
) as dag:
    task1 = BashOperator(task_id="task_1", bash_command="exit 1")
    task2 = BashOperator(task_id="task_2", bash_command="sleep 5")
    task3 = BashOperator(task_id="task_3", bash_command="sleep 5")
    

    task1 >> task2 >> task3