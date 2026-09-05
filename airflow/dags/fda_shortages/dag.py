from airflow_operator import create_dag, DEFAULT_START_DATE
from common_dag_tasks import transform
from fda_shortages.dag_tasks import extract_load

# FDA's drug-shortage database, via openFDA (api.fda.gov/drug/shortages.json).
#
# This is the durable replacement source for the drug-shortage signal that the
# `ashp` DAG can no longer scrape (ashp.org sits behind Cloudflare's managed
# challenge — see the header of ashp/dag.py, whose option 2 is exactly this
# DAG). It is a DIFFERENT dataset with different fields and different
# semantics: FDA reports one row per package NDC per company, where ASHP
# reported one row per shortage bulletin. Nothing here reads or writes the
# ashp tables, and consumers of stg_ashp__* migrate deliberately, not via a
# shim.
dag_id = "fda_shortages"

dag = create_dag(
    dag_id=dag_id,
    # Daily at 04:30 UTC, after the 04:00 cluster (ashp/fda_ndc et al.) —
    # openFDA refreshes this dataset roughly daily.
    schedule="30 4 * * *",
    start_date=DEFAULT_START_DATE,
    catchup=False,
    max_active_runs=1,
    concurrency=2,
)

with dag:
    extract_load_task = extract_load()
    transform_task = transform(dag_id)

    extract_load_task >> transform_task
