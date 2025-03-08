"""
    Atividade 10 AWARI
    Aluno: Paulo Jorge
    Curso: Engenharia de Dados
"""
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from covid19_data_download_from_source_operator import Covid19DataDownloadFromSourceOperator

GITHUB_BASE_URL = "https://github.com/owid/covid-19-data/blob/master/public/data/"

dag1 =  DAG(dag_id=f"ingest_covid19_dag",start_date=datetime(2021,1,1),schedule_interval="@daily", catchup=False)

download_task = Covid19DataDownloadFromSourceOperator(
    task_id="download_covid19_data", url=GITHUB_BASE_URL, dag=dag1
)

download_task