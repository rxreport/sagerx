import pendulum
from airflow_operator import create_dag
from airflow.utils.helpers import chain

from common_dag_tasks import  extract, get_ordered_sql_tasks, get_ds_folder
from sagerx import read_sql_file
from airflow.providers.postgres.operators.postgres import PostgresOperator


dag_id = "orange_book"

dag = create_dag(
    dag_id=dag_id,
    # FIXED start_date. The shared default is a sliding `pendulum.today()`, which
    # is re-evaluated on every parse (every 30s), so a data interval can never
    # close and the DAG never runs — this DAG last ran 2026-04. A fixed date is
    # the only thing that unfreezes it. catchup=False keeps this to ONE run.
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    schedule= "15 0 24 * *",  # monthly (field 4 is the MONTH: "1" meant January-only) — on the 24th day at 00:15
    max_active_runs=1,
    concurrency=2,
)

with dag:
    url = "https://www.fda.gov/media/76860/download"
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id,url)

    task_list = [extract_task]
    for sql in get_ordered_sql_tasks(dag_id):
        sql_path = ds_folder / sql
        task_id = sql[:-4] #remove .sql

        sql_task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="postgres_default",
            sql=read_sql_file(sql_path).format(data_path=extract_task),
            dag=dag
        )
        task_list.append(sql_task)
    
    chain(*task_list) 
   