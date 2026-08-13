import pendulum
from airflow.decorators import dag
from umls.dag_tasks import extract, load
from common_dag_tasks import transform
from airflow_operator import DEFAULT_START_DATE

dag_id = "umls"

@dag(
    dag_id=dag_id,
    schedule_interval="0 6 15 * *",  # Runs on the 15th of each month at 3 AM
    start_date=DEFAULT_START_DATE,
    catchup=False
)
def umls():
    extract_task = extract(dag_id)
    load_task = load(extract_task)
    transform_task = transform(dag_id, models_subdir=['staging', 'intermediate'])

    extract_task >> load_task >> transform_task

dag = umls()
