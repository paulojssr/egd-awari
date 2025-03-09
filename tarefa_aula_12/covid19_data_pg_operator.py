"""
    Atividade 12 AWARI
    Aluno: Paulo Jorge
    Curso: Engenharia de Dados
"""

import requests
import os
import pandas as pd
from io import StringIO, BytesIO

from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from custom_s3_hook_covid19 import CustomS3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Lista de arquivos e diretórios para download
files_to_download = [
    "locations-age.parquet",
    "locations-manufacturer.parquet",
    "locations.parquet",
    "us_state_vaccinations.parquet",
    "vaccinations-by-age-group.parquet",
    "vaccinations-by-manufacturer.parquet",
    "vaccinations.parquet",
    "vaccinations.parquet",
]

class BIPgOperator(BaseOperator):
    def __init__(self, tablename: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tablename = tablename
        self.custom_s3 = CustomS3Hook(bucket="covid-19-data")
        self.pg_hook = PostgresHook(postgres_conn_id="pg_awari")
        self.pg_conn = self.pg_hook.get_conn()
        self.engine = self.pg_hook.get_sqlalchemy_engine()
        self.current_time = datetime.now()
        self.current_date = self.current_time .strftime("%Y-%m-%d")

    def execute(self, context):
        for file_name in files_to_download:
            self.process_to_pg(file_name)
        return self.tablename
    
    def process_to_pg(self, file_name):
        print("Fazendo download do arquivo: " + file_name)
        
        # Obter o objeto do S3 (ou MinIO)
        parquet_obj = self.custom_s3.get_object(key=f"datalake/{file_name}")
        
        # Ler o conteúdo do objeto StreamingBody e criar um buffer em memória
        parquet_buffer = BytesIO(parquet_obj.read())
        
        # Ler o arquivo Parquet a partir do buffer
        df = pd.read_parquet(parquet_buffer)
        
        # Salvar o DataFrame em um arquivo CSV temporário
        temp_csv_path = "/tmp/temp_file.csv"
        df.to_csv(temp_csv_path, index=False)
        
        # Ler o CSV de volta para um DataFrame (opcional)
        df_from_csv = pd.read_csv(temp_csv_path)
        
        # Escrever o DataFrame no PostgreSQL
        df_from_csv.to_sql(self.tablename, con=self.engine, if_exists='replace', index=False)
        
        # Remover o arquivo CSV temporário (opcional)
        os.remove(temp_csv_path)
        
        print("Dados inseridos no PostgreSQL com sucesso.")
