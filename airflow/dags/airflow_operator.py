from airflow import DAG
from airflow.models.param import Param

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel
    
def create_dag(dag_id,**kwargs) -> DAG:
    import pendulum
    from datetime import timedelta

    dag_args ={
        "dag_id":dag_id,
        # Was `days_ago(0)`, which emits RemovedInAirflow3Warning on every DAG
        # parse (33 DAGs, re-parsed continuously) and is removed in Airflow 3.
        # `pendulum.today("UTC")` is the exact same value: days_ago(0) returns
        # utcnow() truncated to midnight. Verified equal in the deployed
        # container (both -> 2026-08-12T00:00:00+00:00).
        "start_date": pendulum.today("UTC"),
        "schedule": "0 5 * * *",  # run at 5am every day
        "description": f"Processes {dag_id} source",
    }

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "email": ["admin@sagerx.io"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "retrieve_dataset_function": get_dataset,
        "on_failure_callback": alert_slack_channel,
        "dagrun_timeout":60
    }

    dag_args.update(kwargs)
    default_args.update(kwargs)

    # `concurrency` was renamed `max_active_tasks` in Airflow 2.2 and is removed
    # in Airflow 3; passing it warns on every parse. 23 DAGs in this repo pass
    # `concurrency=N`, so translating it HERE fixes all of them without touching
    # a single call site — and DAG() maps the two to the same attribute anyway
    # (verified in the deployed container: both forms yield max_active_tasks=N,
    # only the old spelling warns). Callers may still pass either spelling.
    if "concurrency" in dag_args:
        dag_args.setdefault("max_active_tasks", dag_args.pop("concurrency"))
    # default_args is applied to OPERATORS, which accept neither name; leaving a
    # stale `concurrency` there is harmless but pointless, so drop it too.
    default_args.pop("concurrency", None)

    dag = DAG(**dag_args,default_args=default_args)

    return dag