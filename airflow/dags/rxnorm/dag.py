from datetime import timedelta
from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

from airflow.decorators import dag, task

from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

from common_dag_tasks import extract, transform, run_subprocess_command


@dag(
    schedule="0 0 10 * *",
    start_date=pendulum.datetime(2005, 1, 1),
    catchup=False,
)
def rxnorm():
    dag_id = "rxnorm"
    api_key = Variable.get("umls_api")
    ds_url = f"https://uts-ws.nlm.nih.gov/download?url=https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_current.zip&apiKey={api_key}"

    extract_task = extract(dag_id, ds_url)

    # Task to load data into source db schema
    #
    # ⚠ The load tasks run in PARALLEL and each one opens with
    # `DROP TABLE ... CASCADE`. Several dbt staging views read more than one of
    # these tables (stg_rxnorm__atc_codes, __ingredient_strengths and __ndcs
    # each read BOTH rxnconso and rxnsat — measured on the warehouse
    # 2026-10-08), so two concurrent cascades must each take
    # AccessExclusiveLock on the SAME views and can take them in opposite
    # orders. Postgres then aborts one as a
    # deadlock victim. That is what failed this DAG on 2026-10-06:
    #   load_rxnsat: DeadlockDetected — waits for AccessExclusiveLock on a
    #   pg_rewrite object held by the load_rxnconso cascade, and vice versa.
    # A deadlock victim is by definition safe to re-run: Postgres rolled it
    # back and the other side has proceeded. With `default_task_retries = 0`
    # there was no second attempt, so one lost lock race cost the week's load
    # and turned data/AIRFLOW_DAG_HEALTH red. Each load is idempotent (DROP,
    # CREATE, COPY), so retrying it is exactly the same as running it once.
    # A file or SQL error still fails the task — after the retries.
    load = []
    ds_folder = Path("/opt/airflow/dags") / dag_id
    for sql in get_sql_list("load", ds_folder):
        sql_path = ds_folder / sql
        task_id = sql[:-4]
        load.append(
            PostgresOperator(
                task_id=task_id,
                postgres_conn_id="postgres_default",
                sql=read_sql_file(sql_path),
                retries=2,
                retry_delay=timedelta(minutes=2),
            )
        )

    transform_task = transform(dag_id, models_subdir=['staging', 'intermediate'])

    extract_task >> load >> transform_task

rxnorm()
