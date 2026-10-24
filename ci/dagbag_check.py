#!/usr/bin/env python3
"""Fail unless every DAG under airflow/dags/ imports cleanly inside the Airflow image.

Why this exists
---------------
A DAG that fails to import does not fail anything. The scheduler records it in
the `import_error` table, the UI shows a banner nobody is looking at, and the
DAG simply stops being scheduled: no failed run, no red task, nothing the
warehouse's AIRFLOW_DAG_HEALTH check (which reads dag runs) can see. On an
upgrade that changes import paths (2.5.1 -> 2.11.2 -> 3.x), that is every DAG
at once.

It has also happened without an upgrade. On 2026-10-18 `vsac/test_json_retry.py`
ended in `sys.exit()`, and every CLI command that loads the whole DagBag
(`airflow dags list`, `list-import-errors`) stopped at that file and exited 0,
listing two DAGs of 32 (sagerx #24). An exit-0 that checked two DAGs reads
exactly like an exit-0 that checked all of them.

So this builds the DagBag the way the scheduler does, inside the image the
scheduler runs, and checks:

  1. no import errors;
  2. every directory under dags/ that holds a dag.py produced at least one DAG
     (the expected set is DERIVED from disk, never a hand-kept count, so a new
     DAG directory is covered the day it is added);
  3. building the DagBag did not raise past it (a module-level `sys.exit()` is
     a SystemExit, which DagBag does not catch; caught here and reported as a
     failure, never as the exit code it carried);
  4. no test_*.py is a DAG candidate: the `.airflowignore` fence from sagerx #24
     is checked through Airflow's own file listing, not by reading the file.

One named exception: a DAG in PARSE_TIME_WAREHOUSE queries the warehouse while
it is being parsed, which production can answer and CI cannot. It may fail with
the unreachable-warehouse error and nothing else, and the entry goes STALE (and
fails) the moment it imports without the warehouse or its directory goes away.

ci/dagbag_mutants.py breaks each of these on purpose and requires a red run.

Usage
-----
    python3 ci/dagbag_check.py [DAGS_DIR]      # default /opt/airflow/dags

It imports airflow, so run it inside the airflow image (.github/workflows/ci.yml
does). Exit codes: 0 clean · 1 violation found · 2 usage · 4 could not look.
Python 3.7 compatible (the 2.5.1 image).
"""
from __future__ import annotations

import sys
from pathlib import Path

DEFAULT_DAGS = "/opt/airflow/dags"

# The warehouse address CI gives `postgres_default` (.github/workflows/ci.yml). `.invalid`
# can never resolve (RFC 2606), so a DAG that reaches the warehouse while being parsed
# fails with exactly this host in its error.
UNREACHABLE_WAREHOUSE = "warehouse.invalid"

# DAG directories whose dag.py QUERIES THE WAREHOUSE at import time. In production the
# warehouse answers, so they import; here it cannot, so they fail. Each is allowed to
# fail ONLY with the unreachable-warehouse error above. Named, never hidden.
PARSE_TIME_WAREHOUSE = {
    "dbt_gcp": (
        "load_to_gcs() runs at module level and reads sagerx.data_availability to build "
        "one task pair per table, so every parse sweep queries the warehouse and prints "
        "each table and its columns, though the DAG is paused (see its own docstring)"
    ),
}


def expected_dag_dirs(dags: Path) -> list:
    """Every immediate subdirectory of dags/ that holds a dag.py."""
    return sorted(p.parent.name for p in dags.glob("*/dag.py"))


def test_scripts(dags: Path) -> list:
    """Every test_*.py anywhere under dags/, found WITHOUT Airflow's ignore rules."""
    return sorted(p for p in dags.rglob("test_*.py") if "__pycache__" not in p.parts)


def error_line(err: str) -> str:
    """The line of a formatted traceback that names the exception, not a doc link."""
    lines = [ln.strip() for ln in str(err).splitlines()]
    keep = [ln for ln in lines if ln and not ln.startswith(("(Background", "File ", "Traceback", "^"))]
    return keep[-1] if keep else str(err).strip()


def owning_dir(fileloc: str, dags: Path) -> str:
    try:
        parts = Path(fileloc).resolve().relative_to(dags).parts
    except ValueError:
        return "<outside dags/>"
    return parts[0] if len(parts) > 1 else "<top level>"


def main(argv: list) -> int:
    if len(argv) > 2 or (len(argv) == 2 and argv[1].startswith("-")):
        print("usage: python3 ci/dagbag_check.py [DAGS_DIR]", file=sys.stderr)
        return 2
    dags = Path(argv[1] if len(argv) == 2 else DEFAULT_DAGS).resolve()

    if not dags.is_dir():
        print(f"COULD NOT LOOK: {dags} is not a directory")
        return 4
    expected = expected_dag_dirs(dags)
    if not expected:
        print(f"COULD NOT LOOK: no */dag.py under {dags}; refusing to report a clean parse of nothing")
        return 4

    try:
        import airflow
        from airflow.models import DagBag
        from airflow.utils.file import list_py_file_paths
    except Exception as exc:  # the image is wrong, not the DAGs
        print(f"COULD NOT LOOK: airflow is not importable here ({type(exc).__name__}: {exc})")
        return 4

    print(f"airflow {airflow.__version__} · python {sys.version.split()[0]} · dags {dags}")
    print(f"expected: {len(expected)} DAG directories (every */dag.py on disk)")

    failures = []

    # 4. The fence. safe_mode=False skips the "mentions airflow and dag" heuristic, so
    #    the listing is the ignore rules alone; a test script that survives it is one
    #    the scheduler imports whenever its text happens to match the heuristic.
    scripts = test_scripts(dags)
    candidates = {str(Path(p).resolve()) for p in list_py_file_paths(str(dags), safe_mode=False, include_examples=False)}
    leaked = [s for s in scripts if str(s.resolve()) in candidates]
    for s in leaked:
        failures.append(f"not ignored: {s.relative_to(dags)} is a DAG candidate (check dags/.airflowignore)")
    print(f"fence: {len(scripts)} test_*.py on disk, {len(leaked)} visible to the DagBag")

    # 3. Build the bag the scheduler's way. Anything that escapes it is a failure,
    #    including SystemExit(0): a clean exit code from a DAG file is not a clean parse.
    try:
        bag = DagBag(dag_folder=str(dags), include_examples=False, safe_mode=True)
    except BaseException as exc:  # SystemExit and friends are the point
        print(f"FAIL: building the DagBag raised {type(exc).__name__}: {exc!r}")
        print("      a DAG file (or something it imports) stopped the whole parse; nothing after it was checked")
        return 1

    # 1. Import errors, with the named parse-time-warehouse DAGs held to their one excuse.
    allowed = {}
    for path, err in sorted(bag.import_errors.items()):
        d = owning_dir(path, dags)
        if d in PARSE_TIME_WAREHOUSE and UNREACHABLE_WAREHOUSE in str(err):
            allowed[d] = error_line(err)
            continue
        failures.append(f"import error: {d}: {error_line(err)}")

    # 2. Coverage of the derived set, and the exception list held to the tree.
    by_dir = {}
    for dag_id, dag in bag.dags.items():
        by_dir.setdefault(owning_dir(dag.fileloc, dags), []).append(dag_id)
    for d in expected:
        if d not in by_dir and d not in allowed:
            failures.append(f"no DAG: {d}/dag.py produced no DAG")
    for d in sorted(PARSE_TIME_WAREHOUSE):
        if d not in expected:
            failures.append(f"STALE allowance: {d} is in PARSE_TIME_WAREHOUSE but {d}/dag.py does not exist")
        elif d in by_dir:
            failures.append(f"STALE allowance: {d} imported without the warehouse; remove it from PARSE_TIME_WAREHOUSE")

    print()
    for d in sorted(by_dir):
        print(f"  {d}: {', '.join(sorted(by_dir[d]))}")
    for d in sorted(allowed):
        print(f"  {d}: NOT PARSED, allowed by name: it queries the warehouse at import ({allowed[d]})")
    extra = sorted(set(by_dir) - set(expected))
    if extra:
        print(f"note: DAGs outside a */dag.py directory: {', '.join(extra)}")

    covered = len([d for d in expected if d in by_dir])
    print(
        f"\nresult: {len(bag.dags)} DAG(s) from {covered}/{len(expected)} directories, "
        f"{len(bag.import_errors)} import error(s), {len(allowed)} of them the named warehouse exception"
    )
    if failures:
        print(f"\nFAIL ({len(failures)}):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
