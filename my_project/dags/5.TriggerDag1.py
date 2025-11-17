import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

#triggers
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "depends_on_past": False,
    "email": ["teste@email.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id="trigger_dag_1",
    description="testando os triggers",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Default Dags", "Sozinho"],
    default_args=default_args
) as dag:
    task1 = BashOperator(task_id="task_1", bash_command="sleep 5", retries=3)
    task2 = BashOperator(task_id="task_2", bash_command="sleep 5")
    task3 = BashOperator(task_id="task_3", bash_command="sleep 5", trigger_rule=TriggerRule.ONE_FAILED)
    

    [task1, task2] >> task3