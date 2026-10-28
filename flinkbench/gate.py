# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""JVM-band gate: is a canonical session fit to anchor the comparison panels?

The cross-engine tables pair a Rust leg with a JVM leg in one run, but the JVM
leg carries its own machine state (JIT/GC warm-up shape per string form), and
history shows it can swing 25-30% on the non-ASCII shapes while the fixed-shape
legs stay put. Ingesting such a session would present Rust as having regressed
against a "faster JVM" that never changed (the M137/M144/M145 discipline: do
not overwrite drifted shape rows). This gate makes that judgement mechanical:
every reference leg in the log is compared against the median of its recent
stored history; a session passes only when no leg leaves the band.
"""

import statistics

from . import ingest

DEFAULT_TOLERANCE = 0.15
MIN_HISTORY = 2
RECENT_WINDOW = 3


def _reference_legs(text):
    """(case, value) pairs for the JVM/pyflink reference legs found in a log."""
    out = []
    for line in text.splitlines():
        for pattern, emit in ingest._COMPILED:
            m = pattern.search(line)
            if m:
                for case, metric, value, _unit, _dir in emit(
                        [m.group(i) for i in range(1, m.lastindex + 1)]):
                    if metric in ("flink_jvm", "pyflink"):
                        out.append((case, float(value)))
                break
    return out


def check(con, text, tolerance=DEFAULT_TOLERANCE, exclude_source=None):
    """Return (ok, rows). rows: (case, session, history_median, deviation, verdict).

    The band is the median of the last `RECENT_WINDOW` stored values for the
    same leg — the *recent* regime, since a harness re-anchor (the M136 shape
    re-pair) legitimately moves a leg's level and old sessions must not vote on
    it. The session under review is excluded by source so it never anchors
    itself.
    """
    rows = []
    ok = True
    for case, value in _reference_legs(text):
        history = [v for (v,) in con.execute(
            """SELECT m.value FROM metrics m JOIN runs r ON r.id = m.run_id
               WHERE (m.case_name = ? AND m.metric = 'flink_jvm'
                   OR m.case_name = ? AND m.metric = 'pyflink')
                 AND (? IS NULL OR r.source != ?)
               ORDER BY r.ts ASC, r.id ASC""", (case, case, exclude_source, exclude_source)).fetchall()]
        history = history[-RECENT_WINDOW:]
        if len(history) < MIN_HISTORY:
            rows.append((case, value, None, None, "no-history"))
            continue
        med = statistics.median(history)
        dev = (value - med) / med
        verdict = "ok" if abs(dev) <= tolerance else "drift"
        if verdict == "drift":
            ok = False
        rows.append((case, value, med, dev, verdict))
    return ok, rows
