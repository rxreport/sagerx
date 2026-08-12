import pendulum
from airflow_operator import create_dag
from airflow.utils.helpers import chain

from common_dag_tasks import  extract, transform, get_ordered_sql_tasks, get_ds_folder
from sagerx import read_sql_file
from airflow.providers.postgres.operators.postgres import PostgresOperator


dag_id = "rxterms"

dag = create_dag(
    dag_id=dag_id,
    # FIXED start_date. The shared default is a sliding `pendulum.today()`, which
    # is re-evaluated on every parse (every 30s), so a data interval can never
    # close and the DAG never runs — this DAG last ran 2026-04. A fixed date is
    # the only thing that unfreezes it. catchup=False keeps this to ONE run.
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    schedule= "45 0 15 * *",  # monthly (field 4 is the MONTH: "1" meant January-only) — on the 15th day at 00:45
    max_active_runs=1,
    concurrency=2,
)   

with dag:
    mnth = "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m' ) }}"
    url = f"https://data.lhncbc.nlm.nih.gov/public/rxterms/release/RxTerms{mnth}.zip"
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id,url)
    transform_task = transform(dag_id)

    sql_tasks = []
    for sql in get_ordered_sql_tasks(dag_id):
        sql_path = ds_folder / sql
        task_id = sql[:-4] #remove .sql

        sql_task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="postgres_default",
            sql=read_sql_file(sql_path).format(data_path=extract_task, mnth=mnth),
            dag=dag
        )
        sql_tasks.append(sql_task)
    
    extract_task >> sql_tasks >> transform_task
   