import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import time
import re
import json
import logging
import threading
import pendulum

from time import sleep
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from pathlib import Path
import shutil
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pandas import json_normalize

# Filesystem functions
def create_path(*args):
    """creates and returns folder path object if it does not exist"""

    p = Path.cwd().joinpath(*args)
    if not p.exists():
        p.mkdir(parents=True)
    return p

def read_sql_file(sql_path: str):
    """reads a sql file and returns the string when given a path
    sql_path = path as string to sql file"""
    
    fd = open(sql_path, "r")
    sql_string = fd.read()
    fd.close()
    return sql_string

def read_json_file(json_path:str):
    with open(json_path, 'r') as f:
        json_object = json.load(f)
    return json_object

def write_json_file(json_path:str, data):
    with open(json_path, 'w') as f:
        json.dump(data, f)

# ── Streaming variants, for a payload too big to hold twice ──────────────────
#
# read_json_file/write_json_file above build the WHOLE structure in memory. That
# is fine for most DAGs here and is deliberately left alone — mccpd, umls and
# fda_enforcement all use them and their formats must not change under them.
#
# It is not fine for rxclass. Measured 2026-09-15: its intermediate file is
# 883 MB of JSON, and `json.load` needs roughly five to ten times a file's size
# in object overhead, so reading it back wants 4.5-9 GB on a box with 7.9 GB.
# The `load` task was OOM-killed (Negsignal.SIGKILL) sixty seconds in, having
# done nothing but open the file — and the kernel log shows the same kill taking
# the `extract` task at 6.0 GB on 2026-09-08.
#
# JSON Lines fixes it without a parser dependency: one object per line means the
# writer never holds more than one record and the reader never holds more than
# one line. The format is INTERNAL to a DAG — whoever writes it reads it — so
# nothing outside needs to agree.
#
# MEASURED, on 361 MB of rxclass-shaped records, same row count from both:
#
#   json.load on the array   1855 MB peak RSS   (5.1x the file)
#   iter_json_lines          12 MB peak RSS     (155x less)
#
# ⚠ The 12 MB only holds while the caller does not materialise the generator.
# `list(iter_json_lines(...))` puts every byte back and is worse than what this
# replaced, because it pays for the objects AND the line parsing.
#
# ⚠ A record containing a literal newline would corrupt the file. `json.dumps`
# escapes newlines inside strings by default (ensure_ascii aside, it never emits
# a raw \n within a JSON string), so this holds for any JSON-serialisable value.
def write_json_lines(json_path:str, records):
    """Write an iterable of JSON-serialisable records, one per line."""
    with open(json_path, 'w') as f:
        for record in records:
            f.write(json.dumps(record))
            f.write('\n')

def iter_json_lines(json_path:str):
    """Yield one record per line. A GENERATOR — the caller must not list() it,
    or the memory this exists to save comes straight back."""
    with open(json_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

# Web functions

# Several federal data hosts sit behind a bot-management WAF (FDA/accessdata is on
# Akamai Bot Manager) that refuses the default `python-requests/x.y` User-Agent.
# The refusal is NOT an honest 404/403 for the resource: FDA answers a request for a
# file that plainly exists with **HTTP 404 and a ~420-byte "excessive requests"
# apology page**, which is indistinguishable from "this month was never published"
# unless you look at the body. Verified 2026-08-12 from inside the airflow container
# against a file confirmed present in FDA's own download index:
#   default UA -> 404 (418-byte text/html)   browser UA -> 200 (476,652-byte csv)
# Every outbound fetch in this repo must therefore send a browser User-Agent. This is
# the same header `download_dataset` has always sent; it is defined here so probe
# code (which historically forgot it) can share the one definition.
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
)


def browser_headers() -> dict:
    """Headers every outbound dataset request should carry. See BROWSER_USER_AGENT."""
    return {"User-Agent": BROWSER_USER_AGENT}


def download_dataset(url: str, dest: Path = Path.cwd(), file_name: str = None):
    """Downloads a data set file from provided Url via a requests steam

    url = url to send request to
    dest = path to save downloaded file to
    filename = name to call file, if None last segement of url is used"""
    import requests
    import re

    headers = browser_headers()

    with requests.get(url, stream=True, allow_redirects=True, headers=headers) as r:
        r.raise_for_status()

        if file_name == None:
            try:
                content_disposition_list = r.headers["Content-Disposition"].split(";")

                compiled_regex = re.compile(
                    r"""
                    # the filename directive keyword
                    filename=
                    # 0 or 1 quote
                    (?:"|')?
                    # capture the filename itself
                    (?P<filename>.+)
                    # a quote or end of string
                    # if not proceded by a quote
                    (?:"|'|(?<!(?:"|'))$)
                    """,
                    re.VERBOSE,
                )

                # get the only element of the content_disposition_list
                # after filtering based on regex pattern
                # NOTE: if 0 or >1 elements after filtering, will return ValueError
                [filename_directive] = list(
                    filter(compiled_regex.search, content_disposition_list)
                )

                match = compiled_regex.search(filename_directive)

                file_name = match.group("filename")

            except:
                file_name = url.split("/")[-1]

        dest_path = create_path(dest) / file_name

        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return dest_path


# Airflow DAG Functions
def get_dataset(ds_url, data_folder, ti=None, file_name=None):
    """retreives a dataset from the web and passes the filepath to airflow xcom
    if file is a zip, it extracts the contents and deletes zip

    ds_url = url to download dataset file from
    data_folder = path to save dataset to
    ti = airflow parameter to store task instance for xcoms"""
    import zipfile
    import logging

    file_path = download_dataset(url=ds_url, dest=data_folder)
    logging.info(f"requested url: {ds_url}")
    if file_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(file_path.with_suffix(""))
        Path.unlink(file_path)
        file_path = file_path.with_suffix("")

    # change name of file/directory if one is provided
    if file_name is not None:
        dest = file_path.with_name(file_name)
        if dest.exists():
            shutil.rmtree(dest) if dest.is_dir() else dest.unlink()
        file_path.rename(dest)
        file_path = dest

    file_path_str = file_path.resolve().as_posix()
    if ti != None:
        ti.xcom_push(key="file_path", value=file_path_str)
    logging.info(f"created dataset at path: {file_path}")
    return file_path_str


def get_sql_list(pre_str: str = "", ds_path: Path = Path.cwd()) -> list:
    """When given a folder path returns all .sql files in it as a list

    pre_str = determines what sql files to grab by matching str at start of name
    ds_path = folder path to grab sqls from"""

    sql_file_list = [path.name for path in ds_path.glob(pre_str + "*.sql")]
    return sql_file_list


# Slack webhook function
def alert_slack_channel(context):
    slack_api = Variable.get("slack_api", default_var=None)
    if not slack_api:
        logging.info("Variable 'slack_api' is not set; skipping Slack failure alert")
        return
    msg = """
            :red_circle: Task Failed
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    SlackWebhookOperator(
        task_id="alert_slack_channel",
        http_conn_id="slack",
        message=msg,
    ).execute(context=None)

def load_df_to_pg(df,schema_name:str,table_name:str,if_exists:str,dtype_name:str="",index:bool=True, 
                  create_index: bool = False, index_columns: list = None) -> None:
    from airflow.hooks.postgres_hook import PostgresHook
    import sqlalchemy

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()

    if dtype_name:
        dtype = {dtype_name:sqlalchemy.types.JSON}
    else:
        dtype = {}
    
    # trying it this way to prevent wiping tables that actually need to append
    if if_exists == 'replace':
        engine.execute(f'drop table if exists {schema_name}.{table_name} cascade')
        if_exists = 'append'

    df.to_sql(
        table_name,
        con=engine,
        schema=schema_name,
        if_exists=if_exists,
        dtype=dtype,
        index=index
    )

    if create_index and index_columns:
        columns_str = ', '.join(index_columns)
        engine.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_{"_".join(index_columns)} ON {schema_name}.{table_name} ({columns_str})')

def run_query_to_df(query:str) -> pd.DataFrame:
    from airflow.hooks.postgres_hook import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, con=engine)
    
    return df

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RateLimiter:
    """
    Restrict calls to a maximum of `max_calls` within `period` seconds.
    Ensures we don't exceed API rate limits.
    """
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()

    def __call__(self, f):
        def wrapped(*args, **kwargs):
            with self.lock:
                now = time.time()
                # Remove calls that fell outside the time window
                self.calls = [c for c in self.calls if now - c < self.period]
                # If we've reached the limit, sleep until we're allowed to call again
                if len(self.calls) >= self.max_calls:
                    sleep_time = self.period - (now - self.calls[0])
                    #logging.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds.")
                    time.sleep(sleep_time)
                self.calls.append(time.time())
            return f(*args, **kwargs)
        return wrapped


@RateLimiter(max_calls=20, period=1)  # Limit to 20 calls/second
def fetch_json(url):
    """
    Fetch JSON from a URL, respecting the rate limit.
    """
    with urlopen(url) as response:
        data = json.loads(response.read())

    # If the body says "Too Many Requests" even though status is 200, treat it like a 429 and retry
    if isinstance(data, dict) and data.get("error") == "Too Many Requests":
        raise HTTPError(url, 429, "Too Many Requests (from response body)", None, None)

    return data


def concurrent_api_calls(url, max_retries=3, initial_delay=1):
    """
    Call the rxclass API for one URL, retrying on rate limits and transport
    errors. Returns {"url", "response"} on success, or None for a concept that
    could not be fetched.

    EVERY ERROR PATH HERE RAISED `NameError: name 'rxcui' is not defined`.
    Five log lines interpolated `rxcui`, which is neither a parameter nor
    otherwise in scope — a leftover from when this took a concept dict rather
    than a URL (the old docstring still said it did).

    That was not a cosmetic logging bug. A NameError raised INSIDE an `except`
    block propagates out of the whole `try`, so the first 429 killed the task
    instead of backing off: the retry this function exists for had never run,
    not once.

    Found 2026-09-15 by the Airflow DAG health check. rxclass had failed three
    consecutive weekly runs (09-01, 09-08, 09-15), last success 08-25, each
    time after ~5000 API calls — i.e. each time on the first rate limit. The
    traceback read as a Python scope error, so the actual cause (HTTP 429 from
    RxNav) sat one layer underneath it and nobody saw it.

    THE HANDLER ORDER WAS WRONG TOO, and silently. `except Exception` sat ABOVE
    `except URLError`, `except (KeyError, TypeError)` and a SECOND
    `except Exception`; Python takes the first compatible clause, so all three
    lower branches were unreachable — and Python does not warn about an
    unreachable except. Specific before general now, and the duplicate is gone.
    """
    for attempt in range(max_retries):
        try:
            response = fetch_json(url)
            return {"url": url, "response": response}

        except HTTPError as e:
            if e.code == 429:
                # Exponential backoff for a rate limit, or for "Too Many
                # Requests" returned inside a 200 body (see fetch_json).
                delay = initial_delay * (2 ** attempt)
                logging.warning(
                    f"Rate limit (429) at {url}. "
                    f"Retrying in {delay} seconds... (Attempt {attempt+1}/{max_retries})"
                )
                time.sleep(delay)
            else:
                # Skip for other HTTP errors
                logging.error(
                    f"HTTP error {e.code} for {url}. Will skip to the next concept."
                )
                return None

        except URLError as e:
            # Transport-level failure: usually transient, so retry.
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt)
                logging.warning(
                    f"URLError at {url}: {e.reason}. "
                    f"Retrying in {delay} seconds... (Attempt {attempt+1}/{max_retries})"
                )
                time.sleep(delay)
            else:
                logging.error(
                    f"URLError at {url}: {e.reason}. "
                    f"Max retries reached. Skipping to the next concept."
                )
                return None

        except (KeyError, TypeError) as e:
            logging.error(
                f"Data structure error for {url}: {e}. Skipping to the next concept."
            )
            return None

        except Exception as e:
            logging.error(
                f"Unexpected error for {url}: {e}. Skipping to the next concept."
            )
            return None

    # If we exhaust all retries, return None (concept failed)
    logging.error(f"Max retries reached for {url}. Skipping to the next concept.")
    return None

def get_concurrent_api_results(url_list: list):
    """
    Fetch every URL concurrently, and REFUSE to return a partial set quietly.

    `concurrent_api_calls` returns None for a concept it could not fetch. This
    function used to `results.append(result)` unconditionally, so those Nones
    were returned AS DATA and counted in the total it printed — a run that
    fetched half of what it asked for still logged a full-looking count.

    That never mattered while the 429 path raised NameError and killed the task
    on the first rate limit: nothing reached here. Repairing the retry is
    exactly what makes it matter, because a rate-limited run now completes.
    Without this, fixing the handler would have traded a loud weekly failure
    for a silent partial load of the drug classification tables — strictly
    worse, since nothing downstream can tell a missing class from a drug that
    genuinely has none.

    So: Nones are dropped rather than returned, the count of skips is always
    logged, and any skip raises. The retry above already absorbs transience
    with three backed-off attempts; a concept still unfetched after that means
    the source is refusing us, and a weekly classification load that cannot be
    complete should say so rather than publish a hole.
    """
    results = []
    skipped = 0
    with ThreadPoolExecutor(max_workers=10) as executor:
        process_func = partial(concurrent_api_calls)
        mapped_results = executor.map(process_func, url_list)

        for i, result in enumerate(mapped_results, start=1):
            if result is None:
                skipped += 1
            else:
                results.append(result)
            if i % 500 == 0:  # Log every 500 concepts
                logging.info(f"Made {i} API calls so far...")

    requested = len(url_list)
    logging.info(
        f"Concurrent API calls: {len(results)} fetched of {requested} requested, "
        f"{skipped} skipped."
    )

    if skipped:
        raise RuntimeError(
            f"{skipped} of {requested} concepts could not be fetched after retries "
            f"(most often a sustained HTTP 429 from the source). Failing rather "
            f"than loading a partial set — a missing class is indistinguishable "
            f"downstream from a drug that has none."
        )

    return results

def get_rxcuis(ttys:list, active_only:bool = False) -> list:
    settings = ''
    if active_only:
        settings += " and suppress = 'N'"
        
    from airflow.hooks.postgres_hook import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()

    ttys_str = ', '.join(f"'{item}'" for item in ttys)
    df = pd.read_sql(
            f"select distinct rxcui from sagerx_lake.rxnorm_rxnconso where tty in ({ttys_str}) and sab = 'RXNORM'{settings}",
            con=engine
        )
    rxcuis = list(df['rxcui'])

    print(f"Number of RXCUIs: {len(rxcuis)}")
    return rxcuis

def get_rxcuis_from_rxnorm_api(ttys:list) -> list:
    ttys_str = '+'.join(ttys)

    # NOTE: this API seems to only return ACTIVE RXCUIs
    # this is important to note for things like RxNorm Historical
    # which probably requires more than just currently active RXCUIs
    base_url = f"https://rxnav.nlm.nih.gov/REST/allconcepts.json?tty={ttys_str}"

    json = fetch_json(base_url)
    concepts = json['minConceptGroup']['minConcept']
    rxcuis = [concept['rxcui'] for concept in concepts]

    print(f"Number of RxCUIs: {len(rxcuis)}")
    return rxcuis

# Function to convert camelCase to snake_case
def camel_to_snake(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

# Function to convert free-text column names into snake_case
def free_text_to_snake(name: str) -> str:
    """Convert free-text column names into snake_case."""
    if name is None:
        return ""
    name = str(name).strip()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^0-9a-zA-Z_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.lower().strip("_")


# ── HTTP with retries and a TIMEOUT ──────────────────────────────────────────
#
# WHY: a bare `requests.get` has NO timeout and NO retry, so one dropped
# connection kills a whole DAG run. Measured 2026-09-08: the `vsac` DAG failed
# with `ConnectionResetError(104, 'Connection reset by peer')` from
# cts.nlm.nih.gov part-way through its OID loop — and it had failed the same way
# on 08-26 and 08-28 while succeeding in between. That is the shape of a DAG that
# loops over hundreds of requests: the chance that AT LEAST ONE is reset grows
# with the loop, so the run flaps rather than failing honestly. Since the
# warehouse DAG-health beat now pages on a red DAG, a flapping run is not just
# noise — it is what teaches people to ignore the alarm.
#
# ⚠ NO method restriction is passed. urllib3 renamed `method_whitelist` to
# `allowed_methods` in 1.26, so naming either one pins this file to a urllib3
# version (the image is on 1.26.14 today); the default already retries idempotent
# methods, which is every call here.
#
# ⚠ A timeout is NOT optional. Without one a hung peer blocks the worker slot
# until Airflow's own dagrun_timeout (8h) fires — a stall that looks like a slow
# source rather than a dead one.
DEFAULT_HTTP_TIMEOUT = 60

def http_session(total: int = 5, backoff_factor: float = 1.0) -> requests.Session:
    """A requests Session that retries connection errors and 429/5xx with
    exponential backoff. Use it for any external API a DAG loops over."""
    session = requests.Session()
    retry = Retry(
        total=total,
        connect=total,
        read=total,
        status=total,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
