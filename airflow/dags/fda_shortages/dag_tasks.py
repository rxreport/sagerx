from airflow.decorators import task
from airflow.exceptions import AirflowFailException

from fda_shortages.shortages import (
    JSON_DICT_COLUMN,
    COLUMN_ORDER,
    ShortageApiError,
    collect_all_pages,
)


@task(task_id="extract_load")
def extract_load():
    """Page through api.fda.gov/drug/shortages.json and load the full dataset
    into sagerx_lake.fda_shortages (full replace — the endpoint serves the
    complete current state, ~1.6k records as of 2026-09-05, so there is no
    incremental slice to keep).

    Shape decisions (validation, paging, flattening) live in shortages.py;
    this task supplies the HTTP fetcher and the pandas/Postgres load.
    """
    import logging

    import pandas as pd
    import requests

    from sagerx import browser_headers, load_df_to_pg

    def fetch_json(url):
        # requests + browser_headers() directly, the download_dataset
        # precedent — common_dag_tasks.url_request cannot pass headers
        # (it hands them to requests.get positionally, which requests
        # rejects; no caller uses that path today).
        logging.info(f"fetching {url}")
        response = requests.get(url, headers=browser_headers(), timeout=120)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            # Surface the body: federal hosts answer refusals with an
            # explanatory page that the status code alone hides.
            logging.error(f"Response Status Code: {response.status_code}")
            logging.error(f"Response Text: {response.text[:1000]}")
            raise
        try:
            return response.json()
        except ValueError as exc:
            raise AirflowFailException(
                f"{url} answered 200 with a non-JSON body: "
                f"{response.text[:300]!r}"
            ) from exc

    try:
        rows = collect_all_pages(fetch_json)
    except ShortageApiError as exc:
        # A shape change is not transient — fail without retrying, naming
        # the URL and what came back (see shortages.py).
        raise AirflowFailException(str(exc)) from exc

    df = pd.DataFrame(rows)
    # Stable column order: the nineteen observed fields first, then any new
    # upstream additions (flatten_record keeps them) alphabetically.
    extras = sorted(set(df.columns) - set(COLUMN_ORDER))
    df = df[list(COLUMN_ORDER) + extras]
    logging.info(
        f"loading {len(df)} drug-shortage records "
        f"({len(df.columns)} columns) into sagerx_lake.fda_shortages"
    )
    load_df_to_pg(
        df,
        "sagerx_lake",
        "fda_shortages",
        "replace",
        dtype_name=JSON_DICT_COLUMN,
        index=False,
    )
