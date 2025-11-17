import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="teste_de_pools",
    description="teste pools",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "Sozinho"]
) as dag:
    
    task1 = BashOperator(
        task_id="task_leve",
        bash_command="sleep 5",
        pool="meu_pool",
        priority_weight=1,
        weight_rule="absolute"
    )
    
    task2 = BashOperator(
        task_id="task_media",
        bash_command="sleep 5",
        pool="meu_pool",
        pool_slots=2,
        priority_weight=5,
        weight_rule="absolute"
    )
    
    task2 = BashOperator(
        task_id="task_pesada",
        bash_command="sleep 5",
        pool="meu_pool",
        pool_slots=2,   
        priority_weight=10,
        weight_rule="absolute"
    )
    