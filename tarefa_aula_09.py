import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


dag =  DAG(dag_id=f"sequencia_fibonacci",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)

def calcular_fibonacci(**kwargs):
    # Valor de entrada
    inputValue = 100

    # Os primeiros dígitos da sequência
    sequence = [0,1]

    # continua gerando número até que o próximo seja maior que o valor de entrada
    while True:
        next_value = sequence[-1] + sequence[-2]
        if next_value > inputValue:
            break
        sequence.append(next_value)

    # Saida com números gerados com limite até 100
    print(sequence)

calcular_task = PythonOperator(
    task_id='calcular_fibonacci',
    python_callable=calcular_fibonacci,
    provide_context=True,
    dag=dag
)

calcular_task

dag2 =  DAG(dag_id=f"fatorial_de_1024",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)

def calcular_fatorial(**kwargs):
    # Valor de entrada
    inputValue = 1024

    result = 1
    for i in range(2,(inputValue + 1)):
        result = result * i

    # Saida com o produto do fatorial de 1024
    print(result)

calcular_task2 = PythonOperator(
    task_id='calcular_fatorial',
    python_callable=calcular_fatorial,
    provide_context=True,
    dag=dag2
)

calcular_task2