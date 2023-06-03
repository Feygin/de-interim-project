# import sys
from datetime import datetime
from datetime import timedelta
from pathlib import Path
import pandas as pd
import psycopg2
import psycopg2.extras
import logging
import os

from helpers import ConnectionBuilder

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

# sys.path.append(str(Path(__file__).absolute().parent))

log = logging.getLogger(__name__)

def upload_from_s3_to_pg(conn_id: str, sql_path: str, **context) -> None:

    # получаем дату запуска дага из контекста
    dag_start_date = context['execution_date']

    # находим последний актуальный файл из s3
    # пример ссылки 'https://storage.yandexcloud.net/hackathon/events-2022-Sep-30-2134.parquet'
    storage_url = f'https://storage.yandexcloud.net/hackathon/events-{dag_start_date.strftime("%Y-%b-%d")}-2134.parquet' 
    
    data_df = pd.read_parquet(storage_url, engine='auto') # читаем parquet в df
    data_list = data_df.to_dict(orient='records') # df в список словарей для загрузки батчем в pg

    # читаем sql скрипты из файлов
    with open(os.path.join(sql_path, 'delete_current_events.sql')) as file: 
        sql_del = file.read()

    with open(os.path.join(sql_path, 'insert_new_events.sql')) as file: 
        sql_insert = file.read()

    # записываем содержимое df в stg
    conn = ConnectionBuilder.pg_conn(conn_id)
    try:
        with conn.cursor() as cur: # каждый курсор отдельная транзакция
            cur.execute(sql_del, {'load_date': dag_start_date.date()}) # удаляем содержимое за день для идемпотентности

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql_insert, data_list) # загружаем содержимое за день
    finally:
        conn.close()



def get_dag() -> DAG:

    # получаем путь до каталога с sql файлами из переменной Airflow
    sql_path = Variable.get('STG_SQL_FILE_PATH')

    with DAG(
        dag_id='event_loader',
        start_date=datetime(2023, 6, 1),
        schedule_interval='@daily',
        default_args={
            'owner': 'team_1',
            'depends_on_past': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
        },
        catchup=False,
        max_active_runs=1,
        description='loads events from s3 to dwh and builds cdm',
        tags=['hope_this_will_work']
    ) as dag:
        
        load_from_s3_to_stg = PythonOperator(
            task_id='load_from_s3_to_stg',
            python_callable=upload_from_s3_to_pg,
            op_kwargs={
                'conn_id': 'DWH_CONN_ID',
                'sql_path': sql_path
            }
        )

        load_from_s3_to_stg

    return dag


_ = get_dag()
