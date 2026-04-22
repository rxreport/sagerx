from datetime import date

import requests
from dateutil.relativedelta import relativedelta

from airflow.decorators import task
from airflow.utils.helpers import chain
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow_operator import create_dag
from common_dag_tasks import get_ordered_sql_tasks, get_ds_folder, get_data_folder
from sagerx import read_sql_file, get_dataset
from purple_book.dag_tasks import modify_csv

dag_id = "purple_book"

dag = create_dag(
    dag_id=dag_id,
    schedule="15 0 24 1 *",  # monthly on the 24th
    max_active_runs=1,
    concurrency=2,
)


@task
def extract_with_month_fallback() -> str:
    # FDA's Purple Book URL embeds the release month. The current month's file
    # may not exist until FDA publishes it — fall back month-by-month until we
    # find one that resolves.
    data_folder = get_data_folder(dag_id)
    today = date.today()
    attempts = []
    for months_back in range(0, 4):
        candidate = today - relativedelta(months=months_back)
        year = candidate.strftime("%Y")
        month = candidate.strftime("%B")
        url = (
            f"https://www.accessdata.fda.gov/drugsatfda_docs/PurpleBook/"
            f"{year}/purplebook-search-{month}-data-download.csv"
        )
        try:
            head = requests.head(url, timeout=15, allow_redirects=True)
        except Exception as e:
            attempts.append(f"{url} — probe error: {e}")
            continue
        if head.status_code == 200:
            print(f"Found PurpleBook release: {url}")
            return get_dataset(url, data_folder)
        attempts.append(f"{url} — HTTP {head.status_code}")
    raise RuntimeError(
        "No PurpleBook release found in last 4 months:\n" + "\n".join(attempts)
    )


with dag:
    ds_folder = get_ds_folder(dag_id)
    extract_task = extract_with_month_fallback()
    modify_task = modify_csv(extract_task)

    task_list = [extract_task, modify_task]

    for sql in get_ordered_sql_tasks(dag_id):
        sql_path = ds_folder / sql
        task_id = sql[:-4]
        sql_task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="postgres_default",
            sql=read_sql_file(sql_path).format(data_path=extract_task),
            dag=dag,
        )
        task_list.append(sql_task)

    chain(*task_list)
