import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="empty_dag",
    description="teste empty operator",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "Sozinho"]
) as dag:
    
    task1 = BashOperator(task_id="task_1", bash_command="sleep 5")
    task2 = BashOperator(task_id="task_2", bash_command="sleep 5")
    with TaskGroup("tasks") as task_group:
        task3 = BashOperator(task_id="task_3", bash_command="sleep 5")
        task4 = BashOperator(task_id="task_4", bash_command="sleep 5")
        task5 = BashOperator(task_id="task_5", bash_command="sleep 5")
    
    inicio = EmptyOperator(task_id="start")
    fim = EmptyOperator(task_id="end")

    inicio >> [task1, task2] >> task_group >> fim