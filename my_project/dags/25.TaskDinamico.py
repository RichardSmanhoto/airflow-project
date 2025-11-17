import pendulum
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta

items = ['sp', 'rj', 'pr', 'mt', 'ma', 'mg', 'sc']


'''
Valores para SCHEDULE

@continuos -> rodar continuamente

* * * * * -> expressão cron (roda a cada minuto)

timedelta(minutes=1) -> utilizando timedelta no schedule


'''

with DAG(
    dag_id="Dinamico",
    description="Dinamicos",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Primeira", "http", "sensor"]
) as dag:
    
    @task
    def baixar(nome: str) -> str:
        print(f"Baixando: {nome}...")
        return nome
    
    @task
    def processar(nome: str) -> str:
        print(f"Processando {nome}...")
        return f"ok: {nome}"
    
    @task
    def consolidar(resultados: list[str]) -> str:
        print("Consolidado: ", resultados)
        
    
    baixados = baixar.expand(nome = items)
    processados = processar.expand(nome=baixados)
    consolidar(processados)