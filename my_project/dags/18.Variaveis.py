import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
    dag_id="teste_de_variavel",
    description="teste variavel",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "Sozinho"]
) as dag:
    
    def print_variable():
        minha_var = Variable.get("variavel_do_richard")
        print(f"O valor da variável: {minha_var}")
    
    task1 = PythonOperator(
        task_id="tsk1",
        python_callable=print_variable
    )
    
    task1