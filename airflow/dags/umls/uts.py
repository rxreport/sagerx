"""UTS (uts-ws.nlm.nih.gov) answer classification for the umls DAG.

Standard library only and side-effect-free at import, so the offline harness
(`test_retired_concepts.py`, beside this file) can load it with no scheduler,
no Airflow and no API key. The fetching itself stays in `sagerx.py`; this
module only decides what a UTS answer MEANS.

WHY THIS EXISTS. On 2026-10-15 the monthly umls run failed on 19 of the 7,839
SNOMEDCT_US concepts whose relations it asks for: UTS answered HTTP 404 for
`/content/current/source/SNOMEDCT_US/<id>/relations`. Those concepts came out of
UTS's own crosswalk minutes earlier and are simply not in NLM's current release
any more. The shared fetch helper treats every unfetched concept as a failure
and refuses to load a partial set, which is right for an outage and wrong for a
concept that no longer exists: no retry will ever bring it back, so the run
could never succeed again.

Omar's decision (2026-10-15): a 404 that is UTS saying "this concept is not in
the current release" means RETIRED -- no rows for that concept, counted and
logged -- while an outage (429, 5xx, a timeout) still fails the run after
retries.

WHAT COUNTS AS RETIRED. All four, or it is an ordinary failure:

  1. HTTP 404 (the caller only asks about 404s);
  2. the URL is one THIS DAG builds, on an endpoint where a missing concept
     legitimately means "no data": UTS host, a `/rest/...` path matching one of
     RETIRABLE_ENDPOINTS exactly, and a concept id of the right SHAPE for its
     source. A malformed id or a wrong source never qualifies;
  3. the body is UTS's own not-found envelope (UTS_NOT_FOUND_BODY). A 404 from
     anything in front of UTS (a gateway, a CDN, an HTML error page, an empty
     body) does not qualify;
  4. not too many of them: more than MAX_RETIRED_FRACTION of a round's concepts
     answering 404 is a BROKEN REQUEST, not retirements, and fails the run.
     That bound is enforced by the caller over the whole round
     (`sagerx._iter_concurrent_api_results`), because no single answer can
     know it.

⚠ (4) is what stands between this rule and a wrong `version` string. UTS
answers an unknown release with the SAME not-found envelope as a missing
concept -- measured on the public semantic-network endpoint, below -- so the
body alone cannot tell "this concept was retired" from "every concept is
missing because we asked the wrong question". Only the proportion can.
"""

import json
import re
from urllib.parse import urlsplit

UTS_HOST = "uts-ws.nlm.nih.gov"

# UTS's not-found envelope, MEASURED 2026-10-15:
#   GET https://uts-ws.nlm.nih.gov/rest/semantic-network/current/TUI/T999
#   -> HTTP 404, application/json,
#      {"name":"NotFoundError","status":404,"message":"404 Resource Not Found"}
# and the identical body for an unknown release (`/semantic-network/2099AA/...`).
# Its 401 has the same envelope: {"name":"UnauthorizedError","status":401,...}.
#
# ⚠ Measured on semantic-network because that is the one UTS endpoint that
# answers WITHOUT an API key; the relations/crosswalk 404 body itself was not
# captured (that needs the key, which lives on the warehouse). Same service, same
# error envelope, so it is expected to match. If it does NOT, nothing is
# silently swallowed: those 404s classify as ordinary failures, the run fails
# exactly as it did before this rule existed, and `sagerx.concurrent_api_calls`
# logs a sample of the body it did get, so the next reader sees the real shape.
UTS_NOT_FOUND_BODY = {"name": "NotFoundError", "status": 404}

# A round with more than this share of its concepts answering 404 is failed as
# a broken request (wrong source, wrong version, a path typo) rather than loaded.
# 2026-10-15: 19 of 7,839 relations (0.24%) and 0 of 1,626 crosswalks.
MAX_RETIRED_FRACTION = 0.05

# How many retired concept ids a log line or the XCom summary names. They are
# public terminology codes (SNOMED CT / MeSH), never patient data, but a list of
# hundreds is noise; the count is the signal.
RETIRED_EXAMPLES = 5

# The endpoints where a 404 for a concept means "no data for it", each pinned to
# the exact path this DAG builds and to the id shape of its source:
#   * SNOMED CT concept ids are 6-18 digits (the SCTID format).
#   * MeSH unique ids are D (descriptor) or C (supplementary concept) + 6 or 9
#     digits.
# Anything else -- another endpoint, another source, an id of the wrong shape --
# is not retirable, and its 404 fails the run as before.
RETIRABLE_ENDPOINTS = (
    ("relations", re.compile(r"^/rest/content/current/source/SNOMEDCT_US/(\d{6,18})/relations$")),
    ("crosswalk", re.compile(r"^/rest/crosswalk/current/source/MSH/([DC]\d{6}(?:\d{3})?)$")),
)

def retirable_concept_id(url):
    """The concept id if `url` is one of RETIRABLE_ENDPOINTS, else None."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if parts.scheme != "https" or parts.hostname != UTS_HOST:
        return None
    for _name, pattern in RETIRABLE_ENDPOINTS:
        match = pattern.match(parts.path)
        if match:
            return match.group(1)
    return None


def is_uts_not_found_body(body):
    """True only for UTS's own not-found envelope (bytes or str)."""
    if isinstance(body, bytes):
        try:
            body = body.decode("utf-8")
        except UnicodeDecodeError:
            return False
    try:
        parsed = json.loads(body)
    except (TypeError, ValueError):
        return False
    if not isinstance(parsed, dict):
        return False
    return all(parsed.get(k) == v for k, v in UTS_NOT_FOUND_BODY.items())


def is_retired_concept(url, body):
    """The per-answer half of the rule, for a 404: our URL, UTS's envelope.

    The proportion half (MAX_RETIRED_FRACTION) is the caller's, over a round.
    """
    return retirable_concept_id(url) is not None and is_uts_not_found_body(body)


def summarize_retired(retired_urls, requested):
    """A small, JSON-serialisable record of one round's retired concepts, for
    the task log and XCom. Ids only; never a URL (they carry the API key)."""
    ids = []
    for url in retired_urls:
        concept_id = retirable_concept_id(url)
        ids.append(concept_id if concept_id is not None else "?")
    return {
        "requested": requested,
        "retired": len(ids),
        "max_retired_fraction": MAX_RETIRED_FRACTION,
        # Numeric order (shorter id first), so the same retirements name the
        # same examples on every run whatever order the fetch finished in.
        "examples": sorted(ids, key=lambda c: (len(c), c))[:RETIRED_EXAMPLES],
    }
