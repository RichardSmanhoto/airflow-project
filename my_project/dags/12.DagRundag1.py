import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="dag_run1",
    description="Dag run 1",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "Sozinho", "trigger_Dag_operator"]
) as dag:
    task1 = BashOperator(
        task_id = "tsk1",
        bash_command = "sleep 5"
    )
    
    task2 = TriggerDagRunOperator(
        task_id = "tsk2",
        trigger_dag_id = "dag_run2",
        conf={"chave": "Airflow is cool!"},
        wait_for_completion = True, # Espera a conclusão da dag disparada
        poke_interval = 5, # Verifica a cada 5 segundos se a dag disparada foi concluída
    )
    
    task1 >> task2