#!/usr/bin/env python3
"""Prove ci/dagbag_check.py can fail: break each thing it checks and require a red run.

A gate is only evidence if it has been seen to go red. Each mutant below copies the
real dags/ tree to a scratch directory, breaks exactly one thing, runs the gate on
the copy, and requires BOTH the exit code AND the message naming that break, so a
mutant killed for the wrong reason does not count as killed. The unmutated copy runs
first and must pass; without it every red result below proves nothing.

The mutants are built from what is on disk (the first DAG directories in sorted
order, every PARSE_TIME_WAREHOUSE entry), never from a hand-picked DAG, so deleting
or renaming one does not quietly retire a mutant. A mutant that cannot be built is a
failure, and fewer than EXPECTED_MUTANTS run is a failure.

Usage
-----
    python3 ci/dagbag_mutants.py [DAGS_DIR]    # default /opt/airflow/dags

Run it inside the airflow image, after dagbag_check.py itself passed (ci.yml).
Exit codes: 0 every mutant killed · 1 a mutant survived or could not be built · 4 could not look.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
CHECK = HERE / "dagbag_check.py"
sys.path.insert(0, str(HERE))
import dagbag_check as gate  # noqa: E402  (PARSE_TIME_WAREHOUSE, expected_dag_dirs)

EXPECTED_MUTANTS = 10

STALE_DAG = '''import pendulum
from airflow import DAG

dag = DAG(dag_id="ci_mutant_stale", start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), schedule=None)
'''


def append(path: Path, text: str) -> None:
    path.write_text(path.read_text() + text)


def prepend(path: Path, text: str) -> None:
    path.write_text(text + path.read_text())


def mutants(expected: list) -> list:
    """(name, mutate(dags), args_fn(dags), want_rc, want_text). want_text may use {d}."""
    plain = [d for d in expected if d not in gate.PARSE_TIME_WAREHOUSE]
    if len(plain) < 3:
        raise RuntimeError(f"need 3 DAG directories outside PARSE_TIME_WAREHOUSE, found {len(plain)}")
    first, second, third = plain[:3]
    out = [
        ("syntax error in a dag.py",
         lambda t: append(t / first / "dag.py", "\ndef (:\n"),
         None, 1, f"import error: {first}:"),
        ("module-level sys.exit(0) in a dag.py",
         lambda t: append(t / second / "dag.py", "\nimport sys\nsys.exit(0)\n"),
         None, 1, "raised SystemExit"),
        ("a dag.py that defines no DAG",
         lambda t: (t / third / "dag.py").write_text('"""airflow dag placeholder, defines nothing"""\n'),
         None, 1, f"no DAG: {third}/dag.py produced no DAG"),
        # Airflow records a duplicate against whichever copy it parses SECOND, and the
        # directory walk's order differs between machines (the copy locally, the original
        # on the GitHub runner), so the kill is the exception's name, not a directory.
        ("a duplicate dag_id",
         lambda t: shutil.copytree(str(t / first), str(t / "zz_ci_duplicate")),
         None, 1, "AirflowDagDuplicatedIdException"),
        (".airflowignore removed",
         lambda t: (t / ".airflowignore").unlink(),
         None, 1, "not ignored: "),
        ("an empty dags directory",
         lambda t: [shutil.rmtree(str(p)) if p.is_dir() else p.unlink() for p in list(t.iterdir())],
         None, 4, "COULD NOT LOOK: no */dag.py"),
        ("airflow not importable",
         lambda t: None,
         lambda t: [sys.executable, "-s", str(CHECK), str(t)], 4, "COULD NOT LOOK: airflow is not importable"),
    ]
    for d in sorted(gate.PARSE_TIME_WAREHOUSE):
        out += [
            (f"{d}: a different import error",
             lambda t, d=d: prepend(t / d / "dag.py", "import sagerx_ci_missing_module\n"),
             None, 1, f"import error: {d}:"),
            (f"{d}: imports without the warehouse",
             lambda t, d=d: (t / d / "dag.py").write_text(STALE_DAG),
             None, 1, f"STALE allowance: {d} imported without the warehouse"),
            (f"{d}: directory removed",
             lambda t, d=d: shutil.rmtree(str(t / d)),
             None, 1, f"STALE allowance: {d} is in PARSE_TIME_WAREHOUSE but {d}/dag.py does not exist"),
        ]
    return out


def run(dags: Path, args=None) -> tuple:
    cmd = args or [sys.executable, str(CHECK), str(dags)]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    return p.returncode, p.stdout


def main(argv: list) -> int:
    src = Path(argv[1] if len(argv) > 1 else gate.DEFAULT_DAGS).resolve()
    expected = gate.expected_dag_dirs(src)
    if not expected:
        print(f"COULD NOT LOOK: no */dag.py under {src}")
        return 4

    with tempfile.TemporaryDirectory(prefix="dagbag-mutants-") as tmp:
        def fresh(name: str) -> Path:
            t = Path(tmp) / name
            shutil.copytree(str(src), str(t), ignore=shutil.ignore_patterns("__pycache__"))
            return t

        rc, out = run(fresh("baseline"))
        if rc != 0:
            print("FAIL: the UNMUTATED tree does not pass, so no mutant below would prove anything:")
            print(out)
            return 1
        print("baseline: unmutated tree passes (rc 0)")

        try:
            cases = mutants(expected)
        except RuntimeError as exc:
            print(f"FAIL: could not build the mutants: {exc}")
            return 1

        survived = 0
        for i, (name, mutate, args_fn, want_rc, want_text) in enumerate(cases):
            t = fresh(f"m{i:02d}")
            try:
                mutate(t)
            except Exception as exc:
                print(f"  SURVIVED  {name}: the mutant could not be built ({type(exc).__name__}: {exc})")
                survived += 1
                continue
            rc, out = run(t, args_fn(t) if args_fn else None)
            if rc == want_rc and want_text in out:
                print(f"  killed    {name} (rc {rc})")
            else:
                survived += 1
                print(f"  SURVIVED  {name}: wanted rc {want_rc} and {want_text!r}, got rc {rc}")
                print("            " + "\n            ".join(out.strip().splitlines()[-8:]))

    total = len(cases)
    print(f"\nmutants: {total - survived}/{total} killed (floor {EXPECTED_MUTANTS})")
    if total < EXPECTED_MUTANTS:
        print(f"FAIL: only {total} mutant(s) ran, below the floor of {EXPECTED_MUTANTS}")
        return 1
    return 1 if survived else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
