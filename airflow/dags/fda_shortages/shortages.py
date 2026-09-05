"""Pure parsing/validation for the openFDA drug-shortages dataset.

This module deliberately imports nothing outside the standard library, so the
offline harness (offline_check.py, run by hand — there is no pytest CI here)
can exercise every decision in this file on a machine with no Airflow and no
pandas installed. The Airflow task in dag_tasks.py is a thin wrapper that
supplies the HTTP fetcher and hands the result to pandas/load_df_to_pg.

Everything below is designed from the OBSERVED shape of the live endpoint,
probed 2026-09-05 (all 1634 records fetched in two pages):

  * envelope: {"meta": {..., "results": {"skip": int, "limit": int,
    "total": int}}, "results": [record, ...]}
  * paging: limit/skip query params (limit max 1000). openFDA also sends a
    Link: rel="next" header (search_after) but with the dataset at 1634
    records plain skip paging is simpler and well inside openFDA's hard
    skip cap of 25000 — MAX_SKIP below fails loudly long before that cap
    can silently truncate the walk.
  * record fields: ten present on every one of the 1634 records
    (REQUIRED_RECORD_FIELDS), nine optional (OPTIONAL_RECORD_FIELDS).
    `therapeutic_category` is a list; `openfda` is a dict; every date field
    is a MM/DD/YYYY string; everything else is a plain string.
"""

import json

FDA_SHORTAGES_ENDPOINT = "https://api.fda.gov/drug/shortages.json"
PAGE_LIMIT = 1000
# openFDA refuses `skip` beyond 25000. If this dataset ever outgrows what
# skip paging can reach, the walk must move to the Link/search_after cursor —
# page_skips() fails loudly rather than silently loading a truncated dataset.
MAX_SKIP = 25000

# Present on all 1634 records observed 2026-09-05. A record missing one of
# these is a shape change and fails the run (the nadac lesson: an upstream
# change must never wear a quieter costume).
REQUIRED_RECORD_FIELDS = (
    "generic_name",
    "status",
    "update_type",
    "update_date",
    "initial_posting_date",
    "package_ndc",
    "company_name",
    "presentation",
    "contact_info",
    "therapeutic_category",
)

# Observed on a subset of records (counts from the 2026-09-05 probe of all
# 1634): dosage_form 1615, openfda 1455-ish, availability, related_info,
# shortage_reason, discontinued_date, related_info_link, change_date,
# resolved_note. Absent means absent from the API record, and loads as NULL.
OPTIONAL_RECORD_FIELDS = (
    "dosage_form",
    "availability",
    "shortage_reason",
    "related_info",
    "related_info_link",
    "discontinued_date",
    "change_date",
    "resolved_note",
    "openfda",
)

COLUMN_ORDER = REQUIRED_RECORD_FIELDS + OPTIONAL_RECORD_FIELDS

# The one column load_df_to_pg stores as a Postgres JSON column (same shape
# as fda_enforcement's load). Every OTHER list/dict value is serialised to a
# JSON string and lands as text — the ashp precedent, cast ::jsonb in staging.
JSON_DICT_COLUMN = "openfda"


class ShortageApiError(ValueError):
    """The endpoint answered with something other than the observed shape."""


def _shown(obj, limit: int = 300) -> str:
    """A truncated repr for error messages, so a failure names what came back
    without dumping megabytes into the log."""
    text = repr(obj)
    if len(text) > limit:
        text = text[:limit] + f"... [{len(text)} chars total]"
    return text


def page_url(skip: int, limit: int = PAGE_LIMIT) -> str:
    return f"{FDA_SHORTAGES_ENDPOINT}?limit={limit}&skip={skip}"


def validate_envelope(payload, url: str):
    """Check the meta/results envelope and return (results, total).

    Raises ShortageApiError naming the URL and what actually came back for:
    a non-dict payload, a missing/odd meta.results block, a missing or
    non-list results key, an empty results list, or a non-dict record.
    """
    if not isinstance(payload, dict):
        raise ShortageApiError(
            f"{url} did not return a JSON object; got {_shown(payload)}"
        )
    meta_results = payload.get("meta", {}).get("results") if isinstance(
        payload.get("meta"), dict
    ) else None
    if not isinstance(meta_results, dict):
        raise ShortageApiError(
            f"{url} envelope is missing meta.results; top-level keys: "
            f"{sorted(payload.keys())}; payload: {_shown(payload)}"
        )
    missing = [k for k in ("skip", "limit", "total") if not isinstance(meta_results.get(k), int)]
    if missing:
        raise ShortageApiError(
            f"{url} meta.results is missing integer key(s) {missing}; "
            f"got {_shown(meta_results)}"
        )
    results = payload.get("results")
    if not isinstance(results, list):
        raise ShortageApiError(
            f"{url} has no results list; top-level keys: "
            f"{sorted(payload.keys())}; payload: {_shown(payload)}"
        )
    if not results:
        raise ShortageApiError(
            f"{url} returned ZERO results (meta.results: {_shown(meta_results)}). "
            "An empty drug-shortage dataset is not plausible; refusing to load it."
        )
    for i, record in enumerate(results):
        if not isinstance(record, dict):
            raise ShortageApiError(
                f"{url} results[{i}] is not an object; got {_shown(record)}"
            )
    total = meta_results["total"]
    if total <= 0:
        raise ShortageApiError(
            f"{url} reports meta.results.total={total}; refusing to load."
        )
    return results, total


def check_page(results, skip: int, limit: int, total: int, url: str) -> None:
    """A page must hold exactly min(limit, total - skip) records — a short or
    long page means the walk and the server disagree about the dataset."""
    expected = max(0, min(limit, total - skip))
    if len(results) != expected:
        raise ShortageApiError(
            f"{url} returned {len(results)} records where {expected} were "
            f"expected (skip={skip}, limit={limit}, total={total})"
        )


def page_skips(total: int, limit: int = PAGE_LIMIT):
    """Every skip offset needed to walk `total` records, or a loud failure if
    skip paging cannot reach them all (openFDA caps skip at MAX_SKIP)."""
    if total <= 0:
        raise ShortageApiError(f"cannot page a dataset of total={total}")
    last_skip = ((total - 1) // limit) * limit
    if last_skip > MAX_SKIP:
        raise ShortageApiError(
            f"dataset total={total} needs skip={last_skip}, beyond openFDA's "
            f"skip cap of {MAX_SKIP}: switch this walk to the Link/"
            "search_after cursor the endpoint advertises before it silently "
            "truncates."
        )
    return list(range(0, total, limit))


def flatten_record(record: dict, url: str) -> dict:
    """One API record -> one flat row dict.

    * every REQUIRED_RECORD_FIELD must be present and non-empty (shape guard);
    * every COLUMN_ORDER key is present in the output (None when absent), so
      the loaded table has the same columns whatever subset a page carries;
    * lists (therapeutic_category) and any dict other than `openfda` are
      serialised to JSON text (staging casts ::jsonb, the ashp precedent);
    * `openfda` stays a dict (or None) for load_df_to_pg's JSON column;
    * unknown new fields are kept, serialised if non-scalar, so an upstream
      addition lands in the lake instead of being dropped.
    """
    missing = [
        k for k in REQUIRED_RECORD_FIELDS
        if record.get(k) in (None, "", [], {})
    ]
    if missing:
        raise ShortageApiError(
            f"{url} record is missing required field(s) {missing}; "
            f"record: {_shown(record)}"
        )
    row = {}
    for key, value in record.items():
        if key == JSON_DICT_COLUMN:
            if not isinstance(value, dict):
                raise ShortageApiError(
                    f"{url} record carries a non-object {JSON_DICT_COLUMN}: "
                    f"{_shown(value)}"
                )
            row[key] = value
        elif isinstance(value, (list, dict)):
            row[key] = json.dumps(value)
        else:
            row[key] = value
    for key in COLUMN_ORDER:
        row.setdefault(key, None)
    return row


def collect_all_pages(fetch_json, limit: int = PAGE_LIMIT):
    """Walk the whole dataset with the supplied fetcher and return the list of
    flattened rows. `fetch_json(url) -> parsed JSON payload` is injected (and
    `limit` is a parameter) so the offline harness can drive this exact walk,
    multi-page included, from captured fixtures.
    """
    first_url = page_url(0, limit)
    payload = fetch_json(first_url)
    results, total = validate_envelope(payload, first_url)
    check_page(results, 0, limit, total, first_url)
    rows = [flatten_record(r, first_url) for r in results]

    for skip in page_skips(total, limit)[1:]:
        url = page_url(skip, limit)
        payload = fetch_json(url)
        page_results, page_total = validate_envelope(payload, url)
        if page_total != total:
            raise ShortageApiError(
                f"{url} reports total={page_total} but the walk started with "
                f"total={total}; the dataset changed mid-walk — retry the run."
            )
        check_page(page_results, skip, limit, total, url)
        rows.extend(flatten_record(r, url) for r in page_results)

    if len(rows) != total:
        raise ShortageApiError(
            f"collected {len(rows)} records but the endpoint reported "
            f"total={total} ({FDA_SHORTAGES_ENDPOINT})"
        )
    return rows
