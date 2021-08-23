from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd 
import random

# default args
default_args = {
    'owner': 'julio-souza',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 9, 10, 23),
    'email': ['juliocnsouzadev@gmail.com'],
    'email_on_failure': False, #airflow config set email config if True
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

# Definindo - DAG (fluxo)
dag = DAG(
    "step-03",                                                                        
    description="mean age by sex [male or female]", 
    default_args=default_args, 
    schedule_interval=timedelta(minutes=2)
)

# comando para efetuar o dowload dos dados 
get_data = BashOperator(
    task_id='get-data',
    bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o /usr/local/airflow/data/train.csv', 
    dag=dag
)

# Função para sortiar entre homens e mulheres 
def sorteia_h_m():
     return random.choice(['male', 'female'])

# PythonOperator para a função - sorteia_h_m
escolhe_h_m = PythonOperator(
    task_id='escolhe-h-m',
    python_callable=sorteia_h_m,
    dag=dag
)    

# Função que realiza a condição entre homens e mulheres 
def condicao_M_ou_F(**context):
     value = context['task_instance'].xcom_pull(task_ids='escolhe-h-m')
     if value == 'male':
         return 'branch_homem'
     if value == 'female':
         return 'branch_mulher'    

# PythonOperator para a - condicao_M_ou_F
male_female = BranchPythonOperator(
    task_id='condicional',
    python_callable=condicao_M_ou_F,
    provide_context=True,
    dag=dag
)


# Função para calcular idade média dos homens
def mean_homem():
    df = pd.read_csv('/usr/local/airflow/data/train.csv') 
    df = df.loc[df.Sex == 'male']
    print(f"Média de idade dos homens no Titanic:  {df.Age.mean()}")


# PythonOperator para a função - mean_homem
branch_homem = PythonOperator(
    task_id='branch_homem',
    python_callable=mean_homem,
    dag=dag
)

# Função para calcular idade média das mulheres
def mean_mulher():
    df = pd.read_csv('/usr/local/airflow/data/train.csv') 
    df = df.loc[df.Sex == 'female']
    print(f"Média de idade das mulheres no Titanic:  {df.Age.mean()}")


# PythonOperator para a função - mean_mulhr
branch_mulher = PythonOperator(
    task_id='branch_mulher',
    python_callable=mean_mulher,
    dag=dag
)


# Definindo o encadeamento da execução
get_data >> escolhe_h_m >> male_female >> [branch_homem, branch_mulher]