import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

#triggers
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "depends_on_past": False,
    "email": ["teste@email.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id="Dag_task_group",
    description="Exemplo de dag Task Group",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Default Dags", "Sozinho"],
    default_args=default_args
) as dag:
    task1 = BashOperator(task_id="task_1", bash_command="sleep 5")
    task2 = BashOperator(task_id="task_2", bash_command="sleep 1")
    task3 = BashOperator(task_id="task_3", bash_command="sleep 8")
    task4 = BashOperator(task_id="task_4", bash_command="sleep 5")
    task5 = BashOperator(task_id="task_5", bash_command="sleep 5")
    task6 = BashOperator(task_id="task_6", bash_command="sleep 5")
    with TaskGroup(group_id="tsk_group") as tsk_group:
        task7 = BashOperator(task_id="task_7", bash_command="sleep 5")
        task8 = BashOperator(task_id="task_8", bash_command="sleep 5")
        task9 = BashOperator(task_id="task_9", bash_command="sleep 5")
    
    

    task1 >> task2
    task3 >> task4
    [task2, task4] >> task5 >> task6
    task6 >> tsk_group