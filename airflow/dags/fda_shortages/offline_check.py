"""Offline harness for shortages.py — run by hand, no scheduler involved:

    python3 airflow/dags/fda_shortages/offline_check.py

Exercises every decision in shortages.py (envelope validation, paging plan,
record flattening, the full page walk) against fixtures/shortages_page.json,
which is a REAL captured response subset: meta verbatim from the live
endpoint (2026-09-05) and ten real records chosen to cover every optional
field plus one record with no openfda block. Standard library only; exits
non-zero if any case fails. Everything at module level is side-effect-free.
"""

import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import shortages  # noqa: E402
from shortages import (  # noqa: E402
    COLUMN_ORDER,
    REQUIRED_RECORD_FIELDS,
    ShortageApiError,
    check_page,
    collect_all_pages,
    flatten_record,
    page_skips,
    page_url,
    validate_envelope,
)

FIXTURE = HERE / "fixtures" / "shortages_page.json"
URL = "https://example.invalid/probe"

PASSED = 0
FAILED = []


def case(name, fn):
    global PASSED
    try:
        fn()
    except AssertionError as exc:
        FAILED.append(f"{name}: {exc}")
        print(f"FAIL  {name}: {exc}")
    else:
        PASSED += 1
        print(f"ok    {name}")


def expect_error(fn, *fragments):
    try:
        fn()
    except ShortageApiError as exc:
        text = str(exc)
        for fragment in fragments:
            assert fragment in text, f"error lacks {fragment!r}: {text}"
        return
    raise AssertionError("expected ShortageApiError, none raised")


def main():
    fixture = json.loads(FIXTURE.read_text())
    records = fixture["results"]

    # --- envelope -----------------------------------------------------------
    def envelope_accepts_fixture():
        results, total = validate_envelope(fixture, URL)
        assert len(results) == 10, len(results)
        assert total == 1634, total
    case("envelope accepts the captured fixture", envelope_accepts_fixture)

    case("envelope rejects a non-dict payload",
         lambda: expect_error(lambda: validate_envelope([1], URL), URL))
    case("envelope rejects missing meta.results",
         lambda: expect_error(
             lambda: validate_envelope({"results": records}, URL),
             URL, "meta.results"))
    case("envelope rejects meta.results without integer total",
         lambda: expect_error(
             lambda: validate_envelope(
                 {"meta": {"results": {"skip": 0, "limit": 10}},
                  "results": records}, URL),
             URL, "total"))
    case("envelope rejects a missing results list",
         lambda: expect_error(
             lambda: validate_envelope(
                 {"meta": fixture["meta"], "results": None}, URL),
             URL, "no results list"))
    case("envelope rejects ZERO results",
         lambda: expect_error(
             lambda: validate_envelope(
                 {"meta": fixture["meta"], "results": []}, URL),
             URL, "ZERO results"))
    case("envelope rejects a non-object record",
         lambda: expect_error(
             lambda: validate_envelope(
                 {"meta": fixture["meta"], "results": ["x"]}, URL),
             URL, "results[0]"))

    # --- page arithmetic ----------------------------------------------------
    def counts():
        assert page_skips(1634, 1000) == [0, 1000]
        assert page_skips(1000, 1000) == [0]
        assert page_skips(1, 1000) == [0]
        assert page_url(1000) == (
            "https://api.fda.gov/drug/shortages.json?limit=1000&skip=1000")
    case("page arithmetic matches the observed dataset", counts)

    case("skip cap failure is loud and names the cursor alternative",
         lambda: expect_error(lambda: page_skips(26001, 1000),
                              "skip cap", "search_after"))
    case("short page is rejected with the counts",
         lambda: expect_error(lambda: check_page(records, 0, 1000, 1634, URL),
                              URL, "10 records where 1000 were expected"))

    # --- flattening ---------------------------------------------------------
    def flatten_all():
        date_shape = re.compile(r"^\d{2}/\d{2}/\d{4}$")
        for record in records:
            row = flatten_record(record, URL)
            for column in COLUMN_ORDER:
                assert column in row, f"{column} absent from flattened row"
            got = json.loads(row["therapeutic_category"])
            assert got == record["therapeutic_category"], "list did not round-trip"
            if "openfda" in record:
                assert row["openfda"] == record["openfda"], "openfda dict changed"
            else:
                assert row["openfda"] is None, "absent openfda should be None"
            for field in REQUIRED_RECORD_FIELDS:
                assert row[field] not in (None, ""), f"required {field} empty"
            for field in ("update_date", "initial_posting_date",
                          "discontinued_date", "change_date"):
                if row[field] is not None:
                    assert date_shape.match(row[field]), (field, row[field])
            assert row["generic_name"] == record["generic_name"]
            assert row["status"] == record["status"]
    case("every fixture record flattens faithfully", flatten_all)

    def missing_required():
        broken = dict(records[0])
        del broken["generic_name"]
        expect_error(lambda: flatten_record(broken, URL),
                     URL, "generic_name")
    case("a record missing a required field is rejected by name",
         missing_required)

    def unknown_field_kept():
        extended = dict(records[0])
        extended["brand_new_field"] = {"a": 1}
        row = flatten_record(extended, URL)
        assert json.loads(row["brand_new_field"]) == {"a": 1}
    case("an unknown upstream field is kept, serialised", unknown_field_kept)

    # --- the full walk, with a stub fetcher ---------------------------------
    def make_fetcher(total, limit, rows, tamper=None):
        requested = []

        def fetch(url):
            requested.append(url)
            skip = int(url.split("skip=")[1])
            payload = {
                "meta": {"results": {"skip": skip, "limit": limit,
                                     "total": total}},
                "results": rows[skip:skip + limit],
            }
            if tamper:
                payload = tamper(skip, payload)
            return payload
        return fetch, requested

    def multi_page_walk():
        fetch, requested = make_fetcher(10, 4, records)
        rows = collect_all_pages(fetch, limit=4)
        assert len(rows) == 10, len(rows)
        skips = [int(u.split("skip=")[1]) for u in requested]
        assert skips == [0, 4, 8], skips
        assert [r["package_ndc"] for r in rows] == [
            r["package_ndc"] for r in records], "order not preserved"
    case("three-page walk collects all rows in order", multi_page_walk)

    def total_moves_mid_walk():
        def tamper(skip, payload):
            if skip > 0:
                payload["meta"]["results"]["total"] = 11
            return payload
        fetch, _ = make_fetcher(10, 4, records, tamper)
        expect_error(lambda: collect_all_pages(fetch, limit=4),
                     "changed mid-walk")
    case("a total that moves mid-walk fails loudly", total_moves_mid_walk)

    def page_goes_short():
        def tamper(skip, payload):
            if skip == 4:
                payload["results"] = payload["results"][:1]
            return payload
        fetch, _ = make_fetcher(10, 4, records, tamper)
        expect_error(lambda: collect_all_pages(fetch, limit=4),
                     "1 records where 4 were expected")
    case("a short middle page fails loudly", page_goes_short)

    def empty_first_page():
        fetch, _ = make_fetcher(10, 4, [])
        expect_error(lambda: collect_all_pages(fetch, limit=4),
                     "ZERO results")
    case("an empty first page fails loudly", empty_first_page)

    print(f"\n{PASSED} passed, {len(FAILED)} failed")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
