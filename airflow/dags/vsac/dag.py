from datetime import timedelta
from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

from airflow.decorators import dag, task

from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from common_dag_tasks import run_subprocess_command, extract
from vsac.dag_tasks import main_execution
from airflow_operator import DEFAULT_START_DATE



# ⚠ ONE retry, as every DAG built through airflow_operator.py gets: this one is a
# bare @dag, so it ran with none, and on 2026-10-18 one throttled answer from the
# NLM API failed the day's load outright. `_get_json` waits out a few minutes in
# the request itself; the retry covers a longer outage, 15 minutes later.
@dag(
    schedule="0 3 * * *",
    start_date=DEFAULT_START_DATE,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=15)},
)
def vsac():
    dag_id = "vsac"
    base_url = "https://cts.nlm.nih.gov/fhir"
    ds_url = ""

    extract_load_task = main_execution()

    extract_load_task

vsac()
