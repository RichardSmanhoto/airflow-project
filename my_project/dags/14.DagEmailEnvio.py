import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="teste_email",
    description="teste email",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "Sozinho"]
) as dag:
    EmailOperator(
        task_id="send_email",
        to=["richard.11smanhoto@gmail.com"],
        subject="Teste de email no airflow",
        html_content="<hp>Este é um teste de envio de email pelo Airflow</p>"
    )