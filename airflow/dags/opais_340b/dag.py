"""OPAIS 340B — MANUAL-TRIGGER DAG, and deliberately so.

There is no extract task because HRSA publishes no API for the Covered Entity
Daily Report; an operator downloads the workbook from
https://340bopais.hrsa.gov/Reports and drops it in the DAG's data folder, then
triggers this DAG. `schedule=None` (hence a NULL `next_dagrun`) is that design,
not a DAG that has fallen off the scheduler. See MANUAL_DROP_HELP in
opais_340b/dag_tasks.py, which is also what the load task raises when the
workbook is missing.
"""
import pendulum

from airflow_operator import create_dag
from common_dag_tasks import get_data_folder
from opais_340b.dag_tasks import load

dag_id = "opais_340b"

dag = create_dag(
    dag_id=dag_id,
    # Intentionally unscheduled — see the module docstring above.
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    concurrency=1,
)

with dag:
    data_folder = get_data_folder(dag_id)
    load_task = load(dag_id, data_folder.as_posix())
