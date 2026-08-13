import pendulum

from airflow import DAG
from airflow.models.param import Param

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

# A start_date must be a FIXED point in the past, never derived from now.
#
# Airflow creates the run for a data interval only once that interval has ENDED.
# `days_ago(0)` / `pendulum.today("UTC")` / `pendulum.yesterday()` are recomputed
# on every DAG-file parse — every few minutes — so the interval's start keeps
# moving and the interval never completes. No run is ever created, while the DAG
# reports unpaused, active, no import errors, and a plausible `next_dagrun`.
#
# The damage scales with the schedule, which is why it looked arbitrary: a
# `yesterday()` start reaches back one day, so DAILY DAGs still fire. Weekly,
# monthly, quarterly and annual ones never do.
#
# `2026-08-01` rather than something older on purpose: with `catchup=False` the
# date only has to be far enough back that the most recent interval has closed,
# and a recent date makes an accidental catchup=True a bounded mistake instead
# of a four-year replay.
DEFAULT_START_DATE = pendulum.datetime(2026, 8, 1, tz="UTC")


def create_dag(dag_id,**kwargs) -> DAG:
    from datetime import timedelta

    dag_args ={
        "dag_id":dag_id,
        "start_date": DEFAULT_START_DATE,
        "schedule": "0 5 * * *",  # run at 5am every day
        # airflow.cfg sets catchup_by_default = True. With the fixed start_date
        # above, inheriting that would backfill every missed interval — so the
        # default is pinned off here. This is what makes the fixed date safe,
        # not a tidy-up. A DAG wanting a backfill passes catchup=True itself.
        "catchup": False,
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