"""Proof for `_get_xml`'s blank-response retry — `python3 test_empty_xml_retry.py`.

⚠ NOTHING RUNS THIS AUTOMATICALLY. This repo has no CI and no test runner, so
this is a script you run by hand when you touch `_get_xml`, not a gate. Said
plainly because a guard nobody runs that everyone believes in is worse than none.

It loads `_get_xml` out of `dag_tasks.py` with `ast` rather than importing the
module, because importing it pulls in airflow and reads a `Variable` — neither of
which exists outside the scheduler. Everything else is stubbed.

Each mutant below was verified to go RED before this was committed:
  * `return None` instead of raising     -> cases 2 and 3
  * dropping `.strip()`                  -> case 5
  * no retry loop at all (the old code)  -> cases 1 and 5
"""

import ast, sys, types
from xml.etree import ElementTree as ET

src = open(__file__.replace("test_empty_xml_retry.py", "dag_tasks.py")).read()
tree = ast.parse(src)
fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_get_xml")
consts = [n for n in tree.body if isinstance(n, ast.Assign)
          and getattr(n.targets[0], "id", "") in ("EMPTY_XML_RETRIES", "EMPTY_XML_PAUSE_SECONDS")]

ns = {"ET": ET, "DEFAULT_HTTP_TIMEOUT": 30, "time": types.SimpleNamespace(sleep=lambda s: None), "print": lambda *a, **k: None}
exec(compile(ast.Module(body=consts + [fn], type_ignores=[]), "<x>", "exec"), ns)
get = ns["_get_xml"]

class Resp:
    def __init__(self, text): self.text, self.status_code = text, 200
    def raise_for_status(self): pass

class Session:
    def __init__(self, bodies): self.bodies, self.calls = list(bodies), 0
    def get(self, url, headers=None, timeout=None):
        self.calls += 1
        return Resp(self.bodies.pop(0))

OK = "<root><value>A</value></root>"
fails = []

# 1. a blank body then a good one -> parses, and it actually RETRIED
s = Session(["", OK]); ns["_session"] = s
r = get("u", {}, "w")
if r.find("value").text != "A" or s.calls != 2: fails.append(f"1: retry-then-parse calls={s.calls}")

# 2. every attempt blank -> raises, and exhausts the budget
s = Session(["", "", ""]); ns["_session"] = s
try:
    get("u", {}, "w"); fails.append("2: blank body did NOT raise")
except RuntimeError as e:
    if s.calls != ns["EMPTY_XML_RETRIES"]: fails.append(f"2: calls={s.calls}")
    if "empty body" not in str(e): fails.append(f"2: message lost the reason: {e}")

# 3. unparseable (not merely empty) is retried too, and names itself
s = Session(["<broken", "<broken", "<broken"]); ns["_session"] = s
try:
    get("u", {}, "w"); fails.append("3: junk XML did NOT raise")
except RuntimeError as e:
    if "unparseable" not in str(e): fails.append(f"3: {e}")

# 4. a good body on the first call must NOT retry
s = Session([OK]); ns["_session"] = s
get("u", {}, "w")
if s.calls != 1: fails.append(f"4: happy path retried, calls={s.calls}")

# 5. whitespace-only is diagnosed as a BLANK BODY, not as broken XML. Both are
# retried either way, so asserting the retry alone proves nothing (a mutant that
# drops .strip() survives it) — the reason is the only thing .strip() decides,
# and the reason is what an operator reads at 03:00.
s = Session(["   \n  ", "   ", "  "]); ns["_session"] = s
try:
    get("u", {}, "w"); fails.append("5: whitespace body did NOT raise")
except RuntimeError as e:
    if "empty body" not in str(e): fails.append(f"5: whitespace misdiagnosed as {e}")

print("FAIL: " + "; ".join(fails) if fails else "PASS 5/5")
