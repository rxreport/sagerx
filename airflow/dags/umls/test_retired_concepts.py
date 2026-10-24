"""Proof for the umls retired-concept rule and the shared fetch retries.

    python3 airflow/dags/umls/test_retired_concepts.py

CI runs this on every pull request and push to main (.github/workflows/ci.yml,
inside the Airflow image), finding it by its test_*.py name; it exits non-zero
on any failed case, so a failure blocks the PR. It is still safe to run by hand.
Run it when you touch `concurrent_api_calls`, `_iter_concurrent_api_results`
or `umls/uts.py`.
Standard library only; Python 3.7 (the warehouse's Airflow image) and later.
Exits 1 if any case fails.

It loads the fetch helpers out of `sagerx.py` with `ast` rather than importing
the module, because importing it pulls in airflow, pandas and tenacity, none of
which exists outside the scheduler. `fetch_json` (the one function that touches
the network) is replaced by a scripted fake that raises the same `HTTPError`
urlopen raises, with the body on it, so the code under test reads the body the
way it does in production. `umls/uts.py` is imported as it is.

Fixtures are `fixtures/uts_answers.json`, each carrying its provenance: UTS's
404 and 401 envelopes are MEASURED (2026-10-15), the 200 and the gateway 404 are
constructed and say so.

Everything at module level is a definition. The scheduler's DAG processor
imports any file under dags/ that mentions both "airflow" and "dag", and this
one does; importing it must do nothing.

Each guard was broken on purpose and this script went RED before it was
committed (the PR body lists the mutants and the cases that caught each).
"""

import ast
import io
import json
import logging as _logging
import re
import sys
import threading
import time as _time
import types
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from urllib.error import HTTPError, URLError

HERE = Path(__file__).resolve().parent
SAGERX_PY = HERE.parent / "sagerx.py"
FIXTURES = HERE / "fixtures" / "uts_answers.json"

KEY = "k3y-SHOULD-NEVER-BE-LOGGED"
UTS = "https://uts-ws.nlm.nih.gov/rest"

LOADED = (
    "_API_KEY_IN_URL", "redact_url", "RetiredConcept", "_http_error_body",
    "concurrent_api_calls", "_FETCH_CHUNK", "_iter_concurrent_api_results",
    "get_concurrent_api_results", "get_concurrent_api_results_allowing_retired",
)


def relations_url(concept_id, source="SNOMEDCT_US"):
    return f"{UTS}/content/current/source/{source}/{concept_id}/relations?apiKey={KEY}&pageSize=100"


def crosswalk_url(mesh):
    return (f"{UTS}/crosswalk/current/source/MSH/{mesh}"
            f"?targetSource=SNOMEDCT_US,ICD10CM,ICD9CM&apiKey={KEY}&pageSize=100")


class Recorder:
    """Stands in for `logging` and `time` inside the loaded code."""

    def __init__(self):
        self.lock = threading.Lock()
        self.lines = []
        self.sleeps = []

    def _log(self, level, msg):
        with self.lock:
            self.lines.append((level, str(msg)))

    def info(self, msg): self._log("info", msg)
    def warning(self, msg): self._log("warning", msg)
    def error(self, msg): self._log("error", msg)

    def sleep(self, seconds):
        with self.lock:
            self.sleeps.append(seconds)


class FakeSource:
    """A scripted `fetch_json`: each URL answers its outcomes in order, then
    repeats the last one. An outcome is ("ok", payload), ("http", fixture) or
    ("http_code", code, body) for a status with no fixture, or ("urlerror",
    reason)."""

    def __init__(self, fixtures, script):
        self.fixtures = fixtures
        self.script = script
        self.calls = {}
        self.lock = threading.Lock()

    def __call__(self, url):
        with self.lock:
            n = self.calls.get(url, 0)
            self.calls[url] = n + 1
        outcomes = self.script[url]
        outcome = outcomes[min(n, len(outcomes) - 1)]
        kind = outcome[0]
        if kind == "ok":
            return outcome[1]
        if kind == "http":
            fx = self.fixtures[outcome[1]]
            raise HTTPError(url, fx["status"], "fixture", {}, io.BytesIO(fx["body"].encode("utf-8")))
        if kind == "http_code":
            raise HTTPError(url, outcome[1], "scripted", {}, io.BytesIO(outcome[2].encode("utf-8")))
        if kind == "urlerror":
            raise URLError(outcome[1])
        raise AssertionError(f"unknown outcome {outcome!r}")


def load(recorder, source):
    """Exec the named top-level definitions of sagerx.py into a namespace whose
    `fetch_json`, `logging` and `time` are the fakes."""
    tree = ast.parse(SAGERX_PY.read_text())
    wanted = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in LOADED:
            wanted.append(node)
        elif isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") in LOADED:
            wanted.append(node)
    names = {getattr(n, "name", None) or n.targets[0].id for n in wanted}
    missing = set(LOADED) - names
    if missing:
        raise SystemExit(f"sagerx.py no longer defines {sorted(missing)}: this harness tests nothing until it is updated")
    ns = {
        "re": re, "HTTPError": HTTPError, "URLError": URLError,
        "ThreadPoolExecutor": ThreadPoolExecutor, "partial": partial,
        "logging": recorder, "time": types.SimpleNamespace(sleep=recorder.sleep),
        "fetch_json": source,
    }
    module = ast.Module(body=wanted, type_ignores=[]) if sys.version_info >= (3, 8) else ast.Module(body=wanted)
    exec(compile(module, str(SAGERX_PY), "exec"), ns)
    return ns


def main():
    sys.path.insert(0, str(HERE))
    import uts  # noqa: E402  (umls/uts.py, standard library only)

    fixtures = json.loads(FIXTURES.read_text())
    passed = []
    failed = []

    def check(case, ok, detail=""):
        (passed if ok else failed).append(case if ok else f"{case}: {detail}")

    def run_one(url, outcomes, is_retired=None):
        rec = Recorder()
        src = FakeSource(fixtures, {url: outcomes})
        ns = load(rec, src)
        try:
            result = ns["concurrent_api_calls"](url, is_retired=is_retired)
        except Exception as e:  # noqa: BLE001 -- a raise is a wrong answer, reported per case
            result = e
        return ns, rec, src.calls.get(url, 0), result

    REL = relations_url("85180002")
    OK = ("ok", json.loads(fixtures["relations_ok"]["body"]))

    # ── concurrent_api_calls: one URL ──────────────────────────────────────
    ns, rec, calls, r = run_one(REL, [OK])
    check("01 200 returns the response, one call",
          isinstance(r, dict) and r["response"]["pageCount"] == 1 and calls == 1, f"{r!r} calls={calls}")

    ns, rec, calls, r = run_one(REL, [("http", "not_found")], uts.is_retired_concept)
    check("02 UTS 404 not-found on a relations URL is RETIRED, asked once, no sleep",
          type(r).__name__ == "RetiredConcept" and r.url == REL and calls == 1 and not rec.sleeps,
          f"{r!r} calls={calls} sleeps={rec.sleeps}")

    ns, rec, calls, r = run_one(REL, [("http", "not_found")], None)
    check("03 the same 404 WITHOUT a classifier is a failure (rxclass, rxnorm_historical unchanged)",
          r is None and calls == 1, f"{r!r} calls={calls}")

    ns, rec, calls, r = run_one(REL, [("http", "gateway_404")], uts.is_retired_concept)
    logged = " ".join(m for _, m in rec.lines)
    check("04 an HTML 404 (not UTS's envelope) is a failure, and its body is logged",
          r is None and "404 Not Found" in logged, f"{r!r} log={logged[:200]!r}")

    ns, rec, calls, r = run_one(REL, [("http_code", 404, fixtures["unauthorized"]["body"])], uts.is_retired_concept)
    check("05 a 404 carrying UTS's envelope with another status is a failure", r is None, repr(r))

    ns, rec, calls, r = run_one(REL, [("http_code", 404, '{"name":"RouteMissing","status":404}')], uts.is_retired_concept)
    check("06 a 404 envelope with another error name is a failure", r is None, repr(r))

    wrong_source = relations_url("85180002", source="SNOMEDCT_XX")
    ns, rec, calls, r = run_one(wrong_source, [("http", "not_found")], uts.is_retired_concept)
    check("07 UTS not-found for a WRONG SOURCE is a failure, not a retirement", r is None, repr(r))

    for bad_id in ("8518000x", "123", "85180002/../1"):
        ns, rec, calls, r = run_one(relations_url(bad_id), [("http", "not_found")], uts.is_retired_concept)
        check(f"08 UTS not-found for a malformed id {bad_id!r} is a failure", r is None, repr(r))

    ns, rec, calls, r = run_one(crosswalk_url("D011711"), [("http", "not_found")], uts.is_retired_concept)
    check("09 UTS not-found on a crosswalk MeSH url is RETIRED", type(r).__name__ == "RetiredConcept", repr(r))

    ns, rec, calls, r = run_one(REL, [("http_code", 429, "")])
    check("10 a sustained 429 fails after 3 attempts, backing off",
          r is None and calls == 3 and rec.sleeps == [1, 2, 4], f"{r!r} calls={calls} sleeps={rec.sleeps}")

    ns, rec, calls, r = run_one(REL, [("http_code", 429, ""), OK])
    check("11 a 429 then a 200 succeeds", isinstance(r, dict) and calls == 2, f"{r!r} calls={calls}")

    ns, rec, calls, r = run_one(REL, [("http_code", 502, "<html>Bad Gateway</html>"), OK])
    check("12 a 502 then a 200 succeeds (5xx is RETRIED)",
          isinstance(r, dict) and calls == 2 and rec.sleeps == [1], f"{r!r} calls={calls} sleeps={rec.sleeps}")

    for code in (500, 502, 503, 504):
        ns, rec, calls, r = run_one(REL, [("http_code", code, "")], uts.is_retired_concept)
        check(f"13 a sustained {code} still FAILS after 3 attempts",
              r is None and calls == 3 and rec.sleeps == [1, 2], f"{r!r} calls={calls} sleeps={rec.sleeps}")

    ns, rec, calls, r = run_one(REL, [("urlerror", "timed out"), OK])
    check("14 a transport error then a 200 succeeds", isinstance(r, dict) and calls == 2, f"{r!r} calls={calls}")

    ns, rec, calls, r = run_one(REL, [("urlerror", "timed out")])
    check("15 a sustained transport error fails after 3 attempts", r is None and calls == 3, f"{r!r} calls={calls}")

    ns, rec, calls, r = run_one(REL, [("http_code", 400, "bad")], uts.is_retired_concept)
    check("16 another 4xx fails at once, as before", r is None and calls == 1, f"{r!r} calls={calls}")

    # 17. the key never reaches a log line, on any path that logs a URL
    leaks = []
    for outcomes in ([("http", "gateway_404")], [("http_code", 429, "")], [("http_code", 502, "")],
                     [("urlerror", f"refused for {REL}")], [("http", "not_found")]):
        _, rec, _, _ = run_one(REL, outcomes, uts.is_retired_concept)
        for level, line in rec.lines:
            if KEY in line:
                leaks.append((outcomes[0][0:2], level, line[:120]))
        if not any("apiKey=***" in line for _, line in rec.lines):
            leaks.append((outcomes[0][0:2], "no redacted URL logged at all", ""))
    check("17 the API key never reaches the log (every URL is logged redacted)", not leaks, repr(leaks[:3]))

    # ── a whole round ──────────────────────────────────────────────────────
    def run_round(script, fn="get_concurrent_api_results_allowing_retired", fraction=uts.MAX_RETIRED_FRACTION):
        rec = Recorder()
        src = FakeSource(fixtures, script)
        ns = load(rec, src)
        urls = list(script)
        try:
            if fn == "get_concurrent_api_results":
                return ns[fn](urls), rec, None
            return ns[fn](urls, uts.is_retired_concept, fraction), rec, None
        except Exception as e:  # noqa: BLE001 -- the raise IS the result here
            return None, rec, e

    def round_of(n, retired=0, sustained_502=0):
        script = {}
        ids = [str(100000000 + i) for i in range(n)]
        for i, cid in enumerate(ids):
            if i < retired:
                script[relations_url(cid)] = [("http", "not_found")]
            elif i < retired + sustained_502:
                script[relations_url(cid)] = [("http_code", 502, "")]
            else:
                script[relations_url(cid)] = [("ok", {"result": [{"i": i}]})]
        return script

    out, rec, err = run_round(round_of(100, retired=2))
    ok = (err is None and out is not None and len(out[0]) == 98 and len(out[1]) == 2
          and all(isinstance(r, dict) and "response" in r for r in out[0]))
    check("18 2 of 100 retired: 98 results (no marker among them), 2 retired urls, no raise",
          ok, f"err={err!r} out={(len(out[0]), len(out[1])) if out else None}")

    out, rec, err = run_round(round_of(100, retired=6))
    check("19 6 of 100 retired (> 5%) FAILS the round as a broken request",
          isinstance(err, RuntimeError) and "broken request" in str(err), repr(err))

    out, rec, err = run_round(round_of(100, retired=5))
    check("20 exactly 5 of 100 retired (= 5%) is allowed", err is None and len(out[1]) == 5, repr(err))

    out, rec, err = run_round(round_of(100, retired=1, sustained_502=1))
    check("21 a retirement does not excuse an outage: 1 sustained 502 still fails the round",
          isinstance(err, RuntimeError) and "could not be fetched after retries" in str(err)
          and "broken request" not in str(err), repr(err))

    out, rec, err = run_round(round_of(20, retired=1), fn="get_concurrent_api_results")
    check("22 the plain helper (no classifier) still fails on a UTS 404",
          isinstance(err, RuntimeError) and "1 of 20 concepts could not be fetched" in str(err), repr(err))

    rec = Recorder()
    ns = load(rec, FakeSource(fixtures, {}))
    try:
        list(ns["_iter_concurrent_api_results"]([REL], is_retired=uts.is_retired_concept))
        check("23 a classifier with no bound is refused", False, "no ValueError")
    except ValueError:
        check("23 a classifier with no bound is refused", True)
    except Exception as e:  # noqa: BLE001
        check("23 a classifier with no bound is refused", False, f"raised {e!r} instead of ValueError")

    # ── uts.py on its own ──────────────────────────────────────────────────
    retired_urls = [relations_url(c) for c in ("85180002", "189822004", "24734008", "191124002",
                                               "269363005", "289896006", "154780007")]
    summary = uts.summarize_retired(retired_urls, 7839)
    dumped = json.dumps(summary)
    check("24 the summary counts every retirement, names at most 5 ids, carries no URL or key",
          summary["retired"] == 7 and summary["requested"] == 7839 and len(summary["examples"]) == 5
          and "85180002" in summary["examples"] and KEY not in dumped and "http" not in dumped,
          dumped)

    check("25 http:// (not https) is not UTS", uts.retirable_concept_id(REL.replace("https://", "http://")) is None)
    check("26 another host is not UTS",
          uts.retirable_concept_id(REL.replace("uts-ws.nlm.nih.gov", "uts-ws.nlm.nih.gov.example.com")) is None)
    check("27 a body that is JSON but not an object is not the envelope", not uts.is_uts_not_found_body(b"[404]"))
    check("28 undecodable bytes are not the envelope", not uts.is_uts_not_found_body(b"\xff\xfe"))
    check("29 the measured envelope is accepted as bytes and as text",
          uts.is_uts_not_found_body(fixtures["not_found"]["body"].encode())
          and uts.is_uts_not_found_body(fixtures["not_found"]["body"]))

    total = len(passed) + len(failed)
    if failed:
        print(f"FAIL {len(failed)} of {total}:")
        for f in failed:
            print("  " + f)
        return 1
    print(f"PASS {total}/{total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
