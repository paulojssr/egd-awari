"""
    Atividade 10 AWARI
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

# Lista de arquivos e diretórios para download
files_to_download = [
    "vaccinations/locations-age.csv",
    "vaccinations/locations-manufacturer.csv",
    "vaccinations/locations.csv",
    "vaccinations/us_state_vaccinations.csv",
    "vaccinations/vaccinations-by-age-group.csv",
    "vaccinations/vaccinations-by-manufacturer.csv",
    "vaccinations/vaccinations.csv",
    "vaccinations/vaccinations.json",
]

class Covid19DataDownloadFromSourceOperator(BaseOperator):
    def __init__(self, url: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.url = url
        self.custom_s3 = CustomS3Hook(bucket="covid-19-data")
        self.current_time = datetime.now()
        self.current_date = self.current_time .strftime("%Y-%m-%d")

    def execute(self, context):
        for file_name in files_to_download:
            file_url = f"{self.url}{file_name}"

            # ***************************************
            # Download para diretorio local e envio para o MinIO            
            self.download_file(file_url)

            # ***************************************
            # Salvando em formato .parquet no datalake
            self.process_to_parquet(file_url)

            # ***************************************
            print("Iniciando remoção do arquivo local...")
            if os.path.isfile(f"/opt/airflow/downloads/{os.path.basename(file_url)}"):
                os.remove(f"/opt/airflow/downloads/{os.path.basename(file_url)}")
                print(f"Arquivo {os.path.basename(file_url)} removido com sucesso.")
        
        return self.url

    def download_file(self, file_url):
        print(f"Fazendo download do arquivo: {file_url}")
        response = requests.get(file_url)
        if response.status_code == 200:
            print("Download realizado")
            print("Preparando para gravar no diretório 'downloads'...")
            os.makedirs(os.path.dirname("/opt/airflow/downloads/"), exist_ok=True)
            with open(f"/opt/airflow/downloads/{os.path.basename(file_url)}", 'wb') as f:
                f.write(response.content)
                print(f"Arquivo {os.path.basename(file_url)} gravado com sucesso!")

                print(f"Iniciando envio para MinIO...")
                self.custom_s3.put_object(key=f"downloaded/{self.current_date}/{os.path.basename(file_url)}",buffer=response.content)
                print(f"Envio realizado com sucesso!")
        else:
            print(f"Falha no download: {file_url}")

    def process_to_parquet(self, file_url):
        # Caminho do arquivo CSV
        csv_path = f"/opt/airflow/downloads/{os.path.basename(file_url)}"
        print(f"Fazendo download do arquivo: {csv_path}")
        
        try:
            # Ler o arquivo CSV
            df = pd.read_csv(csv_path, header=0, sep=",", quotechar='"', on_bad_lines='skip', engine='python')

            # Converter DataFrame para Parquet
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow')

            # Enviar o arquivo Parquet para o S3
            self.custom_s3.put_object(
                key=f"datalake/{os.path.basename(file_url).replace('.csv', '.parquet')}",
                buffer=parquet_buffer.getvalue()
            )
            print("Arquivo Parquet enviado com sucesso para o S3.")
    
        except Exception as e:
            print(f"Erro ao processar o arquivo: {e}")
