"""QuicksortRx QUMI — KNOWN BROKEN, and TAGGED as such so that saying so does not
cost the whole DAG-health console.

THE SOURCE IS GONE. `extract` pulls

    https://raw.githubusercontent.com/QuicksortRx/universal-med-ids/main/universal-med-ids.csv

and that repository no longer exists. Checked 2026-09-19:
`gh api repos/QuicksortRx/universal-med-ids` answers 404, and the org publishes
two public repos, neither of them this one. So no scheduled run can succeed
until somebody finds a replacement source — there is no branch or path to
re-point at. The last success (2026-04-22) was a manual run from when the file
was still published; the first scheduled run after it, on 2026-09-15, failed at
`extract` with that 404.

Omar declined to retire the DAG on 2026-09-17, so it stays unpaused and keeps
trying. The `rx-known-broken` tag is what keeps that decision from blinding the
estate: `warehouse/ops/airflow-health.sh` NAMES a tagged DAG in every report but
does not fail the console row on it, so the row can still go red for the NEXT
DAG that breaks. It is also a tripwire — the row goes RED the moment a tagged
DAG succeeds again, which is the signal to delete the tag and this docstring.
"""
import pendulum

from airflow_operator import create_dag, DEFAULT_START_DATE
from airflow.providers.postgres.operators.postgres import PostgresOperator

from common_dag_tasks import  extract, transform, generate_sql_list, get_ds_folder
from sagerx import read_sql_file

dag_id = "quicksortrx_qumi"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 8 15 * *",  # Runs on the 15th of each month at 3 AM
    # Read back out of Airflow's own `dag_tag` table by the DAG-health check.
    # See the module docstring: delete this the day a real source exists.
    tags=["rx-known-broken"],
    start_date=DEFAULT_START_DATE,
    catchup=False,
    concurrency=2,
)

with dag:
    url = "https://raw.githubusercontent.com/QuicksortRx/universal-med-ids/main/universal-med-ids.csv"
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id, url)
    transform_task = transform(dag_id)

    sql_tasks = []
    for sql in generate_sql_list(dag_id):
        sql_path = ds_folder / sql
        task_id = sql[:-4]  # remove .sql
        sql_task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="postgres_default",
            sql=read_sql_file(sql_path).format(data_path=extract_task),
            dag=dag
        )
        sql_tasks.append(sql_task)
        
    extract_task >> sql_tasks >> transform_task

