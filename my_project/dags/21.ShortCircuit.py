import pendulum
import random
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator, get_current_context



with DAG(
    dag_id="shortcircuit_test",
    description="teste shortcircuit",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "Sozinho"]
) as dag:
    
    def gera_qualidade() -> int:
        return random.randint(70, 100)
    
    gera_qualidade_task = PythonOperator(
        task_id="gera_qualidade_task",
        python_callable=gera_qualidade
    )
    
    def qualidade_suficiente() -> bool:
        ctx = get_current_context()
        qualidade = ctx['ti'].xcom_pull(task_ids='gera_qualidade_task')
        return int(qualidade) >= 70
    
    shortcircuit = ShortCircuitOperator(
        task_id='short_circuit_task',
        python_callable=qualidade_suficiente
    )
    
    processa = BashOperator(task_id='maior', bash_command='echo "Processando porque qualidade boa"')
    finaliza = BashOperator(task_id='menor', bash_command='echo "Finalizando porque qualidade boa"')
    
    gera_qualidade_task >> shortcircuit >> processa >> finaliza