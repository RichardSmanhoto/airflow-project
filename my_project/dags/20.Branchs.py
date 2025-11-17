import pendulum
import random
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator, get_current_context



with DAG(
    dag_id="branchs",
    description="teste branchs",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "Sozinho"]
) as dag:
    
    def gera_n_aleatorio():
        return random.randint(1, 100)
    
    gera_n_aleatorio_task = PythonOperator(
        task_id="numero_aleatorio",
        python_callable=gera_n_aleatorio
    )
    
    def avalia_n_aleatorio():
        ctx = get_current_context()
        numero = ctx['ti'].xcom_pull(task_ids='numero_aleatorio')
        
        return 'par_task' if numero % 2 == 0 else 'impar_task'
    
    branch_task = BranchPythonOperator(
        task_id = 'branch_task',
        python_callable=avalia_n_aleatorio
    )
    
    par_task = BashOperator(
        task_id="par_task",
        bash_command='echo "É par"'
    )
    
    impar_task = BashOperator(
        task_id="impar_task",
        bash_command='echo "É impar"'
    )
    
    gera_n_aleatorio_task >> branch_task >> [par_task, impar_task]