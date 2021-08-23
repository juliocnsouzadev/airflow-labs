from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import zipfile

data_path = '/usr/local/airflow/data/microdados_enade_2019/2019/3.DADOS/'
file = data_path + 'microdados_enade_2019.txt'

default_args = {
    'owner': 'julio-souza',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 9, 10, 23),
    'email': ['juliocnsouzadev@gmail.com'],
    'email_on_failure': False,  # airflow config set email config if True
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    "step-04-parallel",
    description="Paralelismos",
    # Argumentos definidos na lista acima
    default_args=default_args,
    # Intervalo de cada execução
    schedule_interval=timedelta(minutes=5)
)

start_processing = BashOperator(
    task_id='start_processing',
    bash_command='echo "Start processing!" ',
    dag=dag
)

task_get_data = BashOperator(
    task_id='get-data',
    bash_command='curl http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o /usr/local/airflow/data/microdados_enade_2019.zip',
    dag=dag
)


def unzip_file():
    with zipfile.ZipFile('/usr/local/airflow/data/microdados_enade_2019.zip', 'r') as zipped:
        zipped.extractall('/usr/local/airflow/data')


task_unzip_file = PythonOperator(
    task_id='unzip-data',
    python_callable=unzip_file,
    dag=dag
)


def filter():
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE', 'QE_I01',
            'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']  # seleciona apenas algumas colunas do dataset
    enade = pd.read_csv(file, sep=";", decimal=",", usecols=cols)
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
    ]
    enade.to_csv(data_path + 'enade_filtro.csv', index=False)


task_apply_filter = PythonOperator(
    task_id='apply-filter',
    python_callable=filter,
    dag=dag
)


def build_center_age():
    idade = pd.read_csv(data_path + 'enade_filtro.csv')
    idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()
    idade[['idadecent']].to_csv(data_path + 'idadecent.csv', index=False)


def build_age_center_quad():
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    # faz o cálculo ao quadrado
    idadecent['idade2'] = idadecent.idadecent ** 2
    idadecent[['idade2']].to_csv(data_path + 'idadequadrado.csv', index=False)


task_cent_age = PythonOperator(
    task_id='build-center-age',
    python_callable=build_center_age,
    dag=dag
)

task_quad_age = PythonOperator(
    task_id='constroi_idade_ao_quadrado',
    python_callable=build_age_center_quad,
    dag=dag
)


def build_civil_state():
    filtro = pd.read_csv(data_path + 'enade_filtro.csv')
    filtro['estcivil'] = filtro.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viúvo',
        'E': 'Outros'
    })
    filtro[['estcivil']].to_csv(data_path + 'estcivil.csv', index=False)


task_civil_state = PythonOperator(
    task_id='build-civil-state',
    python_callable=build_civil_state,
    dag=dag
)


def build_color():
    filtro = pd.read_csv(data_path + 'enade_filtro.csv')
    filtro['cor'] = filtro.QE_I02.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indígena',
        'F': "",
        ' ': ""
    })
    filtro[['cor']].to_csv(data_path + 'cor.csv', index=False)


task_color = PythonOperator(
    task_id='build-skin-color',
    python_callable=build_color,
    dag=dag
)


def join_data():
    filtro = pd.read_csv(data_path + 'enade_filtro.csv')
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadequadrado = pd.read_csv(data_path + 'idadequadrado.csv')
    estcivil = pd.read_csv(data_path + 'estcivil.csv')
    cor = pd.read_csv(data_path + 'cor.csv')

    final = pd.concat([filtro,
                       idadecent,
                       idadequadrado,
                       estcivil,
                       cor],
                      axis=1)

    final.to_csv(data_path + 'enade_tratado.csv', index=False)
    print(final)


task_join = PythonOperator(
    task_id='join-data',
    python_callable=join_data,
    dag=dag
)

start_processing >> task_get_data >> task_unzip_file >> task_apply_filter

task_apply_filter >> [task_cent_age, task_civil_state, task_color]

task_quad_age.set_upstream(task_cent_age)

task_join.set_upstream([task_civil_state, task_color, task_quad_age])
