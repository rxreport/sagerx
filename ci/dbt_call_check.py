#!/usr/bin/env python3
"""Fail if a DAG runs dbt outside the dbt container, or runs a command that cannot fail.

Why this exists
---------------
dbt runs in its own container (`docker exec dbt dbt ...`): dbt-core 1.12 there, and
DBT_TARGET=prod, so models build into `sagerx`. Until 2026-10 the dailymed DAG ran
`dbt` inside the Airflow scheduler instead, and was wrong three ways at once: the
scheduler's dbt-core was 1.5.11; the scheduler has no DBT_TARGET, so the profile's
default `dev` built 17 models into `sagerx_dev` on every run; and it called
`SubprocessHook().run_command`, which RETURNS a failing exit code rather than raising,
so a dbt error left the task green.

dbt-core is STILL installed in the Airflow image, deliberately, until that image is
rebuilt against Airflow's constraints file (a later migration stage). Removing it on its
own was measured on 2026-10-24 and rejected: dbt is what lifts several of the image's
packages above the 2.5.1 base image's pins, so without it pip resolved Jinja2 3.1.2,
sqlparse 0.4.3 and protobuf 3.20.0 where the warehouse runs 3.1.6, 0.4.4 and 4.24.4.
Until then this check is the only thing that stops a DAG reaching for it.

Two rules, over every .py under airflow/dags/ except the test_*.py proofs:

  1. no command list whose first word is "dbt" (an in-process dbt run);
  2. no `.run_command(` call outside common_dag_tasks.py, whose
     run_subprocess_command() raises on a non-zero exit.

It reads the code's syntax tree, not its text, so a string or a comment that mentions
either shape (check_marts_coverage.py's fixtures do) is not a violation.

Usage
-----
    python3 ci/dbt_call_check.py [--self-test]

Exit codes: 0 clean · 1 violation found · 2 usage · 4 could not look.
Python 3.7 compatible (the 2.5.1 image) and later.
"""
from __future__ import annotations

import ast
import sys
import tempfile
from pathlib import Path

HELPER = "common_dag_tasks.py"

# Python 3.7's parser emits ast.Str for a string literal; 3.8+ emits ast.Constant, and
# touching ast.Str there only earns a DeprecationWarning (it is gone in 3.14).
LEGACY_STR = ast.Str if sys.version_info < (3, 8) else None


def str_value(node):
    """A string literal's value, on 3.7 (ast.Str) and on 3.8+ (ast.Constant)."""
    if LEGACY_STR is not None and isinstance(node, LEGACY_STR):
        return node.s
    if isinstance(node, ast.Constant) and isinstance(getattr(node, "value", None), str):
        return node.value
    return None


def violations_in(path: Path, rel: str) -> list:
    tree = ast.parse(path.read_text(), filename=str(path))
    found = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.List, ast.Tuple)) and node.elts and str_value(node.elts[0]) == "dbt":
            found.append(f"{rel}:{node.lineno}: runs dbt in-process; use "
                         "run_subprocess_command(['docker', 'exec', 'dbt', 'dbt', ...])")
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "run_command" and path.name != HELPER):
            found.append(f"{rel}:{node.lineno}: calls .run_command(), which returns a failing exit "
                         f"code without raising; use run_subprocess_command() from {HELPER}")
    return found


def scan(dags: Path) -> tuple:
    files = [p for p in sorted(dags.rglob("*.py"))
             if "__pycache__" not in p.parts and not p.name.startswith("test_")]
    out = []
    for p in files:
        out += violations_in(p, str(p.relative_to(dags)))
    return out, len(files)


def self_test() -> int:
    cases = [
        ("in-process dbt list", "cmd = ['dbt', 'run', '--select', 'x']\n", "x/dag.py", 1),
        ("in-process dbt tuple", "cmd = ('dbt', 'seed')\n", "x/dag.py", 1),
        ("dbt in its container", "cmd = ['docker', 'exec', 'dbt', 'dbt', 'run']\n", "x/dag.py", 0),
        ("a string that mentions it", "s = \"['dbt', 'run']\"\n# ['dbt', 'run']\n", "x/dag.py", 0),
        ("run_command in a DAG", "h = SubprocessHook()\nr = h.run_command(['ls'])\n", "x/dag.py", 1),
        ("run_command in the helper", "r = hook.run_command(command, cwd=cwd)\n", HELPER, 0),
        ("both in one file", "r = h.run_command(['dbt', 'run'])\n", "x/dag.py", 2),
    ]
    failures = 0
    for name, src, rel, want in cases:
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(src)
            got = len(violations_in(p, rel))
        ok = got == want
        failures += not ok
        print(f"  {'ok  ' if ok else 'FAIL'} {name}: {got} violation(s), wanted {want}")
    print(f"\nself-test: {len(cases) - failures}/{len(cases)} passed")
    return 1 if failures else 0


def main(argv: list) -> int:
    if len(argv) > 1 and argv[1] == "--self-test":
        return self_test()
    if len(argv) > 1:
        print("usage: python3 ci/dbt_call_check.py [--self-test]", file=sys.stderr)
        return 2
    dags = Path(__file__).resolve().parents[1] / "airflow" / "dags"
    if not dags.is_dir():
        print(f"COULD NOT LOOK: {dags} is not a directory")
        return 4
    found, scanned = scan(dags)
    if scanned == 0:
        print(f"COULD NOT LOOK: no .py files under {dags}")
        return 4
    if found:
        print(f"dbt call check FAILED ({len(found)}):")
        for f in found:
            print(f"  {f}")
        return 1
    print(f"dbt call check OK: {scanned} file(s) scanned, every dbt run goes through the dbt container")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
