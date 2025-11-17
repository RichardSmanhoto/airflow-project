import pendulum
from airflow import DAG
from airflow.sdk import task

with DAG(
    dag_id="dag_xcom",
    description="Minha primeira dag sem apoio emocional :(",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "Sozinho"]
) as dag:
    @task
    def task_write():
        return {"valorxcom1": 10000}
    
    @task
    def task_read(payload: dict):
        print(f"Valor retorno Xcom: {payload['valorxcom1']}")
        
    task_read(task_write())