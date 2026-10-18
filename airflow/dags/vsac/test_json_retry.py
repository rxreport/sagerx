"""Proof for `_get_json`'s retry: `python3 test_json_retry.py`.

⚠ NOTHING RUNS THIS AUTOMATICALLY, like its sibling `test_empty_xml_retry.py`: this
repo has no CI and no test runner, so it is a script you run by hand when you touch
`_get_json`, not a gate.

It loads `_get_json` out of `dag_tasks.py` with `ast` rather than importing the
module, because importing it pulls in airflow and reads a `Variable`; neither
exists outside the scheduler. Everything else is stubbed.

Each mutant below was verified to go RED before this was committed:
  * `return None` instead of raising            -> cases 2, 3 and 6
  * no status check (parse the 429/5xx body)    -> cases 1 and 6
  * no `raise_for_status` (a 404 is retried)    -> case 5
  * no retry loop at all (the 2026-10-18 code)  -> case 1
  * dropping `.strip()`                         -> case 7
  * one fixed pause instead of the schedule     -> case 8
"""

import ast, json, sys, types

src = open(__file__.replace("test_json_retry.py", "dag_tasks.py")).read()
tree = ast.parse(src)
fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_get_json")
consts = [n for n in tree.body if isinstance(n, ast.Assign)
          and getattr(n.targets[0], "id", "") in ("JSON_RETRIES", "JSON_PAUSE_SECONDS", "RETRYABLE_STATUS")]

slept = []
ns = {"json": json, "DEFAULT_HTTP_TIMEOUT": 30, "time": types.SimpleNamespace(sleep=slept.append),
      "print": lambda *a, **k: None}
exec(compile(ast.Module(body=consts + [fn], type_ignores=[]), "<x>", "exec"), ns)
get = ns["_get_json"]
RETRIES = ns["JSON_RETRIES"]


class HTTPError(Exception):
    pass


class Resp:
    def __init__(self, status, text):
        self.status_code, self.text = status, text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise HTTPError(f"HTTP {self.status_code}")


class Session:
    def __init__(self, answers):
        self.answers, self.calls = list(answers), 0

    def get(self, url, headers=None, timeout=None):
        self.calls += 1
        return Resp(*self.answers.pop(0))


OK = (200, '{"id": "1.2.3", "name": "A"}')
PAGE = (503, "<html><body>Service Unavailable</body></html>")
fails = []


def run(answers):
    slept.clear()
    s = Session(answers)
    ns["_session"] = s
    return s


# 1. a throttled/failing answer then a good one -> parses, and it actually RETRIED
s = run([PAGE, OK])
try:
    r = get("u", {}, "w")
    if r.get("name") != "A" or s.calls != 2:
        fails.append(f"1: retry-then-parse calls={s.calls}")
except Exception as e:
    fails.append(f"1: a 503 then a good answer raised {type(e).__name__}: {e}")

# 2. every attempt blank -> raises, and exhausts the budget
s = run([(200, "")] * RETRIES)
try:
    get("u", {}, "w"); fails.append("2: blank body did NOT raise")
except RuntimeError as e:
    if s.calls != RETRIES: fails.append(f"2: calls={s.calls}")
    if "empty body" not in str(e): fails.append(f"2: message lost the reason: {e}")

# 3. a 200 that is not JSON (an HTML page) is retried, and names itself
s = run([(200, "<html>maintenance</html>")] * RETRIES)
try:
    get("u", {}, "w"); fails.append("3: non-JSON 200 did NOT raise")
except RuntimeError as e:
    if "unparseable" not in str(e): fails.append(f"3: {e}")

# 4. a good body on the first call must NOT retry or pause
s = run([OK])
get("u", {}, "w")
if s.calls != 1 or slept: fails.append(f"4: happy path retried, calls={s.calls} slept={slept}")

# 5. a 404 (a value set that does not exist) raises at once: asking again cannot change it
s = run([(404, "<html>Not Found</html>")] * RETRIES)
try:
    get("u", {}, "w"); fails.append("5: 404 did NOT raise")
except HTTPError:
    if s.calls != 1: fails.append(f"5: a 404 was retried, calls={s.calls}")
except RuntimeError as e:
    fails.append(f"5: a 404 was retried and parsed as a body: {e}")

# 6. throttled on every attempt -> raises naming the STATUS, not a parse error. This is
# the 2026-10-18 shape: `http_session` hands back the last 429/5xx, and parsing its
# error page is what reported a JSONDecodeError instead of what actually happened.
s = run([(429, "Too Many Requests")] * RETRIES)
try:
    get("u", {}, "w"); fails.append("6: 429 throughout did NOT raise")
except RuntimeError as e:
    if "HTTP 429" not in str(e): fails.append(f"6: misdiagnosed as {e}")
    if s.calls != RETRIES: fails.append(f"6: calls={s.calls}")
except HTTPError as e:
    fails.append(f"6: a 429 raised at once instead of being retried ({e})")

# 7. whitespace-only is diagnosed as a BLANK BODY, not as broken JSON (both are retried,
# so the reason is the only thing `.strip()` decides, and it is what an operator reads)
s = run([(200, "  \n ")] * RETRIES)
try:
    get("u", {}, "w"); fails.append("7: whitespace body did NOT raise")
except RuntimeError as e:
    if "empty body" not in str(e): fails.append(f"7: whitespace misdiagnosed as {e}")

# 8. the pauses GROW, so the request outlasts a few minutes of throttling, and there is
# no pause after the last attempt
s = run([PAGE] * RETRIES)
try:
    get("u", {}, "w")
except Exception:
    pass  # whether it raises is cases 2, 3 and 6; this one reads only the pauses
if slept != list(ns["JSON_PAUSE_SECONDS"])[: RETRIES - 1] or sorted(slept) != slept or len(set(slept)) < 2:
    fails.append(f"8: pauses {slept}")

print("FAIL: " + "; ".join(fails) if fails else "PASS 8/8")
sys.exit(1 if fails else 0)
