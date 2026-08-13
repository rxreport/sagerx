#!/usr/bin/env python3
"""Fail if any DAG uses a start_date derived from "now".

Why this exists
---------------
Airflow creates the run for a data interval only once that interval has ENDED.
A start_date computed at parse time — `days_ago(0)`, `pendulum.yesterday()`,
`pendulum.today().add(days=-1)` — is recomputed every few minutes, so the
interval's start keeps moving and the interval never completes. The run is never
created. The DAG reports unpaused, active, no import errors, and a plausible
`next_dagrun` in the future, while firing exactly zero times.

Measured on the RxReport warehouse 2026-08-12: 17 of 33 DAGs had NEVER had a
single scheduled run. `build_marts` was one of them, so every dbt model was
frozen at its last manual build (2026-04-22) while raw ingestion kept running —
`sagerx.pricing` served a NADAC price 40% below `sagerx_lake.nadac`'s.

Nothing goes red when this happens, which is why it needs a checker.

Usage
-----
    python3 check_start_dates.py [--self-test]

Exit codes: 0 clean · 1 violation found · 2 usage · 4 could not look.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

# Every spelling of "relative to now" seen in this repo, plus the two obvious
# variants nobody has written yet. Matching the CALL is what matters — a fixed
# `pendulum.datetime(...)` / `pendulum.parse(...)` is exactly what we want.
RELATIVE_PATTERNS = [
    r"days_ago\s*\(",
    r"pendulum\.yesterday\s*\(",
    r"pendulum\.today\s*\(",
    r"pendulum\.now\s*\(",
    r"datetime\.now\s*\(",
    r"date\.today\s*\(",
]
RELATIVE_RE = re.compile(
    r"start_date\s*=\s*[^,\n]*(?:" + "|".join(RELATIVE_PATTERNS) + r")"
)

# create_dag's own default must be a fixed constant.
OPERATOR_DEFAULT_RE = re.compile(r'"start_date"\s*:\s*DEFAULT_START_DATE')
DEFAULT_CONST_RE = re.compile(r"DEFAULT_START_DATE\s*=\s*pendulum\.datetime\(")


def scan(dags_dir: Path) -> tuple[list[str], int]:
    """Return (violations, files_scanned)."""
    violations: list[str] = []
    # This file holds deliberately-bad fixtures in self_test(); scanning it
    # would make the checker fail on its own proof that it can fail.
    files = [
        p
        for p in sorted(dags_dir.glob("*/dag.py")) + sorted(dags_dir.glob("*.py"))
        if p.name != Path(__file__).name
    ]
    scanned = 0
    for path in files:
        try:
            text = path.read_text()
        except OSError as exc:  # unreadable file is a fault, not a pass
            violations.append(f"{path}: could not read ({exc})")
            continue
        scanned += 1
        for lineno, line in enumerate(text.splitlines(), start=1):
            if RELATIVE_RE.search(line):
                violations.append(
                    f"{path}:{lineno}: start_date derived from now — "
                    f"use DEFAULT_START_DATE\n    {line.strip()}"
                )

    operator = dags_dir / "airflow_operator.py"
    if operator.exists():
        text = operator.read_text()
        if not DEFAULT_CONST_RE.search(text):
            violations.append(
                f"{operator}: DEFAULT_START_DATE must be a fixed "
                f"pendulum.datetime(...)"
            )
        if not OPERATOR_DEFAULT_RE.search(text):
            violations.append(
                f"{operator}: create_dag's default start_date must be "
                f"DEFAULT_START_DATE"
            )
        if '"catchup": False' not in text:
            violations.append(
                f"{operator}: create_dag must default catchup to False — "
                f"airflow.cfg sets catchup_by_default = True, and a fixed "
                f"start_date with catchup on backfills every missed interval"
            )
    else:
        violations.append(f"{operator}: missing")

    return violations, scanned


def self_test() -> int:
    """Prove the checker goes red. A guard that cannot fail checks nothing."""
    import tempfile

    cases = [
        ("days_ago(0)", "    start_date=days_ago(0),", True),
        ("pendulum.yesterday()", "    start_date=pendulum.yesterday(),", True),
        ("today().add", "    start_date=pendulum.today('UTC').add(days=-1),", True),
        ("datetime.now()", "    start_date=datetime.now(),", True),
        ("fixed datetime", '    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),', False),
        ("the constant", "    start_date=DEFAULT_START_DATE,", False),
        ("data_interval_start arg", "    def extract(data_interval_start=None):", False),
    ]
    failures = 0
    for name, line, should_flag in cases:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "somedag").mkdir()
            (root / "somedag" / "dag.py").write_text(line + "\n")
            (root / "airflow_operator.py").write_text(
                'import pendulum\n'
                'DEFAULT_START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")\n'
                '"start_date": DEFAULT_START_DATE\n'
                '"catchup": False\n'
            )
            found, scanned = scan(root)
            # airflow_operator.py is itself scanned, so filter to the dag file
            dag_hits = [v for v in found if "somedag" in v]
            ok = bool(dag_hits) == should_flag
            print(f"  [{'ok' if ok else 'FAIL'}] {name}")
            if not ok:
                failures += 1
            if scanned == 0:
                print("  [FAIL] scanned zero files")
                failures += 1

    # A directory with no DAGs must be a FAULT, never a silent pass.
    with tempfile.TemporaryDirectory() as tmp:
        _, scanned = scan(Path(tmp))
        ok = scanned == 0
        print(f"  [{'ok' if ok else 'FAIL'}] empty dir reports zero scanned")
        if not ok:
            failures += 1

    print(f"\nself-test: {len(cases) + 1 - failures}/{len(cases) + 1} passed")
    return 1 if failures else 0


def main(argv: list[str]) -> int:
    if len(argv) > 1 and argv[1] == "--self-test":
        return self_test()
    if len(argv) > 1:
        print(__doc__)
        return 2

    dags_dir = Path(__file__).resolve().parent
    violations, scanned = scan(dags_dir)

    if scanned == 0:
        print(f"COULD NOT LOOK: no DAG files found under {dags_dir}", file=sys.stderr)
        return 4

    if violations:
        print(f"start_date check FAILED ({len(violations)} problem(s)):\n", file=sys.stderr)
        for v in violations:
            print(f"  {v}", file=sys.stderr)
        print(
            "\nA start_date computed at parse time never lets a data interval "
            "complete, so the DAG never runs — silently. Use DEFAULT_START_DATE "
            "from airflow_operator.",
            file=sys.stderr,
        )
        return 1

    print(f"start_date check OK — {scanned} file(s) scanned, no relative start_date.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
