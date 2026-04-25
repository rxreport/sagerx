# Manual-trigger DAG (schedule=None). HRSA's OPAIS 340B portal serves the
# Public Use File behind a JS-rendered UI with no stable direct download URL,
# so this DAG has no extract task — it expects the operator to stage the
# OPAIS Excel workbook in /opt/airflow/data/opais_340b/ before triggering.
# Triggering without a staged file fails fast with FileNotFoundError in
# dag_tasks._get_latest_excel.
import pendulum

from airflow_operator import create_dag
from common_dag_tasks import get_data_folder
from opais_340b.dag_tasks import load

dag_id = "opais_340b"

dag = create_dag(
    dag_id=dag_id,
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    concurrency=1,
)

with dag:
    data_folder = get_data_folder(dag_id)
    load_task = load(dag_id, data_folder.as_posix())
