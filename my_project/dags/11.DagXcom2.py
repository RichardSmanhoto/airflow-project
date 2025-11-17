import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import get_current_context

with DAG(
    dag_id="dag_xcom_2",
    description="Minha primeira dag sem apoio emocional :(",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "Sozinho"]
) as dag:
    def task_write():
        ti = get_current_context()["ti"] #retorna um dicionário com contexto da execução da task instance
        print(ti)
        ti.xcom_push(key="valorxcom1", value=10000)
        
    def task_read():
        ti = get_current_context()["ti"]
        print(ti)
        valor = ti.xcom_pull(key="valorxcom1", task_ids="tsk_write")
        print(f"valor recuperado: {valor}")
        
        
    task1 = PythonOperator(task_id="tsk_write", python_callable=task_write)
    task2 = PythonOperator(task_id="tsk_read", python_callable=task_read)
    
    task1   >> task2