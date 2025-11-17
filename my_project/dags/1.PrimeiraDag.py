import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="primeira_dag_sozinho",
    description="Minha primeira dag sem apoio emocional :(",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "Sozinho"]
) as dag:
    task1 = BashOperator(task_id="task_1", bash_command="sleep 5")
    task2 = BashOperator(task_id="task_2", bash_command="sleep 5")
    task3 = BashOperator(task_id="task_3", bash_command="sleep 5")
    
    task1 >> task2 >> task3