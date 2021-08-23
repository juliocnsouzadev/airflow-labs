from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

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

# define DAG
dag = DAG(
    'step02',
    description = 'playing around with titanic data',
    default_args = default_args,
    schedule_interval = timedelta(minutes= 2)
)

get_data = BashOperator(
    task_id='get-data',
    bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o ~/train.csv',
    dag=dag,
)

def read_data():
    df = pd.read_csv('~/train.csv')
    return df

def calc_mean_age():
    data = read_data();
    return data.Age.mean()

def print_age(**context):
    mean_age = context['task_instance'].xcom_pull(task_ids='calc-mean-age')
    print(f"The mean age in Titanic was: {mean_age} years")

task_mean_age = PythonOperator(
    task_id = 'calc-mean-age',
    python_callable=calc_mean_age,
    dag = dag
)

task_print_age = PythonOperator(
    task_id = 'print-age',
    python_callable=print_age,
    provide_context = True,
    dag = dag
)

get_data >> task_mean_age >> task_print_age