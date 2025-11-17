import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email

def on_fail(ctx):
    send_email(
        to=["richard.11smanhoto@gmail.com"],
        subject=f"[FAIL] {ctx['ti'].task_id}",
        html_content=f"Falhou"
    )
    
def on_ok(ctx):
    send_email(
        to=["richard.11smanhoto@gmail.com"],
        subject=f"[SUCCESS] {ctx['ti'].task_id}",
        html_content=f"Sucesso!"
    )    

default_args = {
    "depends_on_past": False,
    "email": ["richard.11smanhoto@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    'retries': 1}

with DAG(
    dag_id="email_callback",
    description="teste email",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "Sozinho"],
    default_args=default_args
) as dag:
    task1 = BashOperator(task_id="task_1", bash_command="exit 1", on_failure_callback=on_fail)
    
    task2 = BashOperator(task_id="task_2", bash_command="sleep 5", on_success_callback=on_ok, trigger_rule=TriggerRule.ALL_DONE)

    task1 >> task2
    