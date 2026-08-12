import time
from datetime import date

import requests
from dateutil.relativedelta import relativedelta

from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.utils.helpers import chain
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow_operator import create_dag
from common_dag_tasks import get_ordered_sql_tasks, get_ds_folder, get_data_folder
from sagerx import read_sql_file, get_dataset, browser_headers
from purple_book.dag_tasks import modify_csv

dag_id = "purple_book"

dag = create_dag(
    dag_id=dag_id,
    # Monthly on the 24th. This read `15 0 24 1 *` until 2026-08-12, which is
    # 00:15 on 24 JANUARY — annually, not monthly (field 4 is the month). Airflow
    # agreed: `airflow dags next-execution purple_book` reported 2026-01-24, seven
    # months in the past, so the DAG had not been scheduled since and would not
    # have been until 2027. FDA publishes this file every month.
    schedule="15 0 24 * *",
    max_active_runs=1,
    concurrency=2,
)

# How many months to walk back looking for a published release. FDA publishes
# monthly, so anything past a couple of months means the source has moved, not
# that we are early — but a wide window costs nothing except probe requests.
LOOKBACK_MONTHS = 12
# FDA's WAF rate-limits bursts. Space the probes out; a run only makes a handful.
PROBE_DELAY_SECONDS = 2
# A WAF block is retried rather than believed — see _probe_month.
BLOCK_RETRIES = 3
BLOCK_RETRY_DELAY_SECONDS = 20
# The bot-management interstitial (see BROWSER_USER_AGENT in sagerx.py) is served
# as text/html; a real release is application/octet-stream. Content type is what
# separates "FDA refused us" from "that month is not published yet" — the status
# code does NOT, because the WAF answers 404 for a file that exists.
_APOLOGY_MARKERS = ("apology", "excessive-requests", "abuse-detection")


def _probe(url: str):
    """Probe one candidate URL.

    Returns (outcome, description) where outcome is one of:
      "found"   — a real data file is there
      "absent"  — the server answered honestly that it is not published
      "blocked" — the WAF refused us; this says NOTHING about whether the file
                  exists, and must never be treated as "absent" (see _resolve)
      "error"   — the request itself failed
    """
    try:
        # A streamed GET (closed before the body is read) costs about what a HEAD
        # costs but, unlike HEAD, lets us read the response body to tell FDA's
        # bot-block page apart from a genuine "not published yet" 404.
        response = requests.get(
            url, timeout=30, allow_redirects=True, stream=True, headers=browser_headers()
        )
    except Exception as e:
        return "error", "probe error: {}: {}".format(type(e).__name__, e)

    try:
        content_type = (response.headers.get("Content-Type") or "").lower()
        if response.status_code == 200 and "html" not in content_type:
            return "found", "HTTP 200 {} bytes={}".format(
                content_type or "unknown content-type",
                response.headers.get("Content-Length") or "unknown",
            )
        # Non-file answer. Read a bounded snippet so the log says WHY.
        snippet = ""
        try:
            snippet = next(response.iter_content(2048, decode_unicode=False), b"").decode(
                "utf-8", "replace"
            )
        except Exception:
            pass
        if any(marker in snippet.lower() for marker in _APOLOGY_MARKERS):
            return "blocked", (
                "HTTP {} — FDA bot-management block (apology page), NOT a missing "
                "file. The request was refused, not answered.".format(response.status_code)
            )
        if response.status_code == 200:
            return "absent", "HTTP 200 but content-type {} — not a data file".format(
                content_type or "unknown"
            )
        return "absent", "HTTP {} ({})".format(
            response.status_code, content_type or "no content-type"
        )
    finally:
        response.close()


def _probe_month(url: str, attempts: list):
    """Probe `url`, retrying while the WAF is the thing answering.

    This retry is load-bearing, not politeness. FDA's WAF is intermittent — on
    2026-08-12 the very first probe of a run was blocked while the next request
    two seconds later succeeded. Treating a block as "this month is not
    published" would walk us silently back to an OLDER month and load stale data
    while reporting success, which is worse than failing. So a block is retried,
    and only a clean "absent" moves the search backwards.
    """
    for attempt in range(1, BLOCK_RETRIES + 1):
        outcome, detail = _probe(url)
        print("PurpleBook probe ({}/{}) : {} — {}".format(attempt, BLOCK_RETRIES, url, detail))
        attempts.append("  {} — attempt {}: {}".format(url, attempt, detail))
        if outcome != "blocked":
            return outcome
        if attempt < BLOCK_RETRIES:
            time.sleep(BLOCK_RETRY_DELAY_SECONDS)
    return "blocked"


@task
def extract_with_month_fallback() -> str:
    """Resolve the newest published Purple Book monthly release and download it.

    FDA's URL embeds the release month, and the current month's file does not
    exist until FDA publishes it (early in the FOLLOWING month — the July 2026
    file was last-modified 2026-08-04), so walk backwards until one resolves.
    The path is case-insensitive on FDA's IIS host, which matters because FDA's
    own download index is inconsistent: 2020-2025 are lowercase, 2026 is mostly
    capitalised, and 2026/january is lowercase. Verified 2026-08-12 that both
    `July` and `july` return the identical 476,652-byte file.
    """
    data_folder = get_data_folder(dag_id)
    today = date.today()
    attempts = []
    blocked_months = []
    for months_back in range(0, LOOKBACK_MONTHS):
        candidate = today - relativedelta(months=months_back)
        label = candidate.strftime("%B %Y")
        url = (
            "https://www.accessdata.fda.gov/drugsatfda_docs/PurpleBook/"
            "{}/purplebook-search-{}-data-download.csv".format(
                candidate.strftime("%Y"), candidate.strftime("%B")
            )
        )
        outcome = _probe_month(url, attempts)
        if outcome == "found":
            if blocked_months:
                # We are about to load an older month than the newest one we
                # asked about, and the newer one never actually answered. Say so
                # rather than quietly presenting stale data as current.
                print(
                    "WARNING: resolved {} but never got a straight answer for: {}. "
                    "This release may not be the newest one published.".format(
                        label, ", ".join(blocked_months)
                    )
                )
            print("Resolved PurpleBook release ({}): {}".format(label, url))
            return get_dataset(url, data_folder)
        if outcome == "blocked":
            blocked_months.append(label)
        if months_back < LOOKBACK_MONTHS - 1:
            time.sleep(PROBE_DELAY_SECONDS)

    raise AirflowFailException(
        "No PurpleBook release resolved in the last {} months. Every URL tried, "
        "in order, with what the server actually returned:\n{}\n"
        "If these say 'bot-management block', FDA refused the requests and the "
        "data may well be there — check the User-Agent being sent (see "
        "BROWSER_USER_AGENT in sagerx.py). If they say HTTP 404 with no apology "
        "page, FDA has genuinely moved or renamed the file: the current index is "
        "at https://purplebooksearch.fda.gov/downloads".format(
            LOOKBACK_MONTHS, "\n".join(attempts)
        )
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
