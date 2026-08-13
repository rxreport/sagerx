#!/usr/bin/env python3
"""Fail if build_marts does not build every directory under models/marts/.

Why this exists
---------------
`build_marts.transform_tasks` names its mart directories one `dbt run --select`
line at a time. That is a hand-authored list of a thing that grows, so it stops
covering its own name the moment someone adds a mart — silently, because dbt
exits 0 for the marts it *was* told about and the DAG goes green.

That is not hypothetical. `models/marts/pricing` was never in the list, so
`pricing` / `pricing_historical` were the only marts the DAG did not build. Their
upstream `int_nadac_pricing` refreshed weekly with the nadac DAG while the mart
on top of it stayed frozen — which is how `sagerx.pricing` came to serve a NADAC
price 40% below the lake for NDC 74157001660 (3.90721 vs 5.45560, 2026-08-13).

Derive the list from the filesystem instead of trusting the enumeration.

Usage
-----
    python3 check_marts_coverage.py [--self-test]

Exit codes: 0 covered · 1 a mart is unbuilt · 2 usage · 4 could not look.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

SELECT_RE = re.compile(r"--select['\"],\s*['\"]\+?models/marts/([A-Za-z0-9_]+)")


def marts_on_disk(dbt_root: Path) -> set[str]:
    marts = dbt_root / "models" / "marts"
    if not marts.is_dir():
        return set()
    return {p.name for p in marts.iterdir() if p.is_dir()}


def marts_selected(dag_file: Path) -> set[str]:
    if not dag_file.is_file():
        return set()
    return set(SELECT_RE.findall(dag_file.read_text()))


def check(dbt_root: Path, dag_file: Path) -> tuple[list[str], set[str], set[str]]:
    on_disk = marts_on_disk(dbt_root)
    selected = marts_selected(dag_file)
    problems: list[str] = []

    if not on_disk:
        problems.append(f"COULD NOT LOOK: no mart directories under {dbt_root}/models/marts")
        return problems, on_disk, selected
    if not selected:
        problems.append(f"COULD NOT LOOK: no `--select models/marts/...` found in {dag_file}")
        return problems, on_disk, selected

    for missing in sorted(on_disk - selected):
        problems.append(
            f"models/marts/{missing} exists but build_marts never builds it — "
            f"add a `dbt run --select +models/marts/{missing}` line"
        )
    for ghost in sorted(selected - on_disk):
        problems.append(
            f"build_marts selects models/marts/{ghost}, which does not exist — "
            f"a selector matching nothing is a silent no-op"
        )
    return problems, on_disk, selected


def self_test() -> int:
    import tempfile

    def build(marts: list[str], selected: list[str]):
        tmp = tempfile.mkdtemp()
        root = Path(tmp)
        for m in marts:
            (root / "dbt" / "sagerx" / "models" / "marts" / m).mkdir(parents=True)
        dag = root / "dag.py"
        dag.write_text(
            "\n".join(
                f"    run_subprocess_command(['docker','exec','dbt','dbt','run',"
                f"'--select', '+models/marts/{s}'], cwd='/dbt/sagerx')"
                for s in selected
            )
            or "# nothing\n"
        )
        return root / "dbt" / "sagerx", dag

    cases = [
        ("all covered", ["ndc", "pricing"], ["ndc", "pricing"], False),
        ("a mart unbuilt", ["ndc", "pricing"], ["ndc"], True),
        ("selector matches nothing", ["ndc"], ["ndc", "gone"], True),
        ("no marts on disk -> fault", [], ["ndc"], True),
        ("no selectors -> fault", ["ndc"], [], True),
    ]
    failures = 0
    for name, marts, selected, should_flag in cases:
        dbt_root, dag = build(marts, selected)
        problems, _, _ = check(dbt_root, dag)
        ok = bool(problems) == should_flag
        print(f"  [{'ok' if ok else 'FAIL'}] {name}")
        failures += 0 if ok else 1
    print(f"\nself-test: {len(cases) - failures}/{len(cases)} passed")
    return 1 if failures else 0


def main(argv: list[str]) -> int:
    if len(argv) > 1 and argv[1] == "--self-test":
        return self_test()
    if len(argv) > 1:
        print(__doc__)
        return 2

    here = Path(__file__).resolve().parent          # airflow/dags
    dbt_root = here.parents[1] / "dbt" / "sagerx"   # repo/dbt/sagerx
    dag_file = here / "build_marts" / "dag.py"

    problems, on_disk, selected = check(dbt_root, dag_file)
    if problems:
        fault = any(p.startswith("COULD NOT LOOK") for p in problems)
        print("marts coverage FAILED:\n", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        return 4 if fault else 1

    print(
        f"marts coverage OK — {len(on_disk)} mart(s) on disk, all built by "
        f"build_marts: {', '.join(sorted(on_disk))}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
