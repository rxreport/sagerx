import pendulum

from airflow import DAG
from airflow.models.param import Param

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

# A DAG's start_date MUST be a fixed point in the past, never a value derived
# from "now" at parse time.
#
# Airflow schedules the run for a data interval only once that interval has
# ENDED. A relative start_date (`days_ago(0)`, `pendulum.yesterday()`,
# `pendulum.today().add(days=-1)`) is recomputed on every DAG-file parse — every
# few minutes — so the interval's start keeps moving forward and the interval
# never gets a chance to complete. The run is never created, `next_dagrun` is
# always in the future, and the DAG shows as unpaused, active and healthy while
# firing exactly zero times.
#
# The damage scales with the schedule: a `yesterday()` start_date reaches back
# one day, so a DAILY schedule still fires. Anything weekly, monthly, quarterly
# or annual never does. Measured on the RxReport warehouse 2026-08-12: 17 of 33
# DAGs had NEVER had a single scheduled run — including `build_marts`, which
# builds every dbt model, so `sagerx.pricing` sat 3.5 months behind
# `sagerx_lake.nadac` and served a NADAC price 40% below the real one.
#
# `catchup=False` below is what makes a fixed past start_date safe: Airflow
# creates only the most recent interval instead of backfilling to 2024.
DEFAULT_START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")


def create_dag(dag_id, **kwargs) -> DAG:
    from datetime import timedelta

    dag_args = {
        "dag_id": dag_id,
        "start_date": DEFAULT_START_DATE,
        "schedule": "0 5 * * *",  # run at 5am every day
        # airflow.cfg sets catchup_by_default = True. Combined with the fixed
        # start_date above that would backfill every missed interval since 2024
        # on the next parse, so the default is pinned off here. A DAG that
        # genuinely wants a backfill passes catchup=True explicitly.
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
        "dagrun_timeout": 60,
    }

    dag_args.update(kwargs)
    default_args.update(kwargs)

    dag = DAG(**dag_args, default_args=default_args)

    return dag
