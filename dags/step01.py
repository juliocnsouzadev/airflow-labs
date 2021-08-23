from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# default args
default_args = {
    'owner': 'julio-souza',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 8, 21, 23),
    'email': ['juliocnsouzadev@gmail.com'],
    'email_on_failure': False, #airflow config set email config if True
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

# define DAG
dag = DAG(
    'step01',
    description = 'basic bash and python operators',
    default_args = default_args,
    schedule_interval = timedelta(minutes= 2)
)

# add tasks
first_task = BashOperator(
    task_id = 'task001',
    bash_command = 'echo "airflow from bash"',
    dag = dag
)

def simple_print ():
    print('airflow from python')
second_task = PythonOperator(
    task_id = 'task002',
    python_callable = simple_print,
    dag = dag
)

first_task >> second_task