import sys
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(str(Path(__file__).absolute().parent))


def get_dag() -> DAG:
    import src

    with DAG(
        dag_id='temp_name',
        start_date=datetime(2023, 6, 1),
        schedule_interval='*/5 * * * *',
        default_args={
            'owner': 'team_1',
            'depends_on_past': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
        },
        catchup=False,
        max_active_runs=1,
        description='temp_description',
        tags=[],
    ) as dag:
        task_1 = PythonOperator(
            task_id='task_1',
            python_callable=src.task_1.main,
        )
        task_2 = PythonOperator(
            task_id='task_2',
            python_callable=src.task_2.main,
        )
        task_3 = PythonOperator(
            task_id='task_3',
            python_callable=src.task_3.main,
        )

        task_1 >> [task_2, task_3]

    return dag


_ = get_dag()
