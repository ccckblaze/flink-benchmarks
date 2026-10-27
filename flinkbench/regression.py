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
"""Regression detection ported from upstream `regression_report_v2.py`.

Same rule in one line: a regression is reported when the last sample moves
against the best baseline value by more than max(min_ratio,
instability_multiplier * last_relative_stddev). Direction-aware so both
throughput and latency suites work; runs entirely off the local sqlite store,
no Codespeed server involved.
"""

MIN_SAMPLE_SIZE_LIMIT = 5


def check_history(values, direction, min_ratio=0.05, instability_multiplier=2.0, baseline_size=10):
    """Return (regressed, detail) or (False, None) when history is too short.

    `values` are oldest-first floats; the upstream semantics (compare against
    the best sustainable baseline, current = best of the last three) are kept
    by working in a "higher is better" space.
    """
    if len(values) < MIN_SAMPLE_SIZE_LIMIT:
        return None
    sign = 1.0 if direction == "higher_is_better" else -1.0
    hb = [sign * v for v in values]
    baseline = hb[-(baseline_size + 2):-2]
    if len(baseline) < MIN_SAMPLE_SIZE_LIMIT:
        return None
    baseline_best = max(baseline)
    current_best = max(hb[-3:])
    last_std = _stddev(hb[-3:])
    current_rel_instability = (last_std / abs(current_best)) if current_best else 0.0
    move = (baseline_best - current_best) / abs(baseline_best) if baseline_best else 0.0
    threshold = max(min_ratio, instability_multiplier * current_rel_instability)
    if move > threshold:
        return True, {
            "baseline_best": sign * baseline_best,
            "current_best": sign * current_best,
            "regression_ratio": move,
            "threshold": threshold,
        }
    return False, None


def _stddev(xs):
    if len(xs) < 2:
        return 0.0
    mean = sum(xs) / len(xs)
    var = sum((x - mean) ** 2 for x in xs) / (len(xs) - 1)
    return var ** 0.5


def scan_store(con, **kwargs):
    """Yield (case, metric, status, detail) for every stored key."""
    out = []
    # Derived ratio series (jvm-over-rust style) mix two measurements from one session;
    # JVM-side drift alone can move them past the threshold, so they are reported in the
    # tables but never judged here. Only raw measured series get a regression verdict.
    derived = {"flink_over_flinkable", "transfer_ratio", "end_to_end_ratio"}
    for case, metric in con.execute(
        "SELECT DISTINCT case_name, metric FROM metrics ORDER BY case_name, metric"
    ).fetchall():
        if metric in derived:
            continue
        values = [v for (v,) in con.execute(
            """SELECT m.value FROM metrics m JOIN runs r ON r.id = m.run_id
               WHERE m.case_name = ? AND m.metric = ? ORDER BY r.ts ASC, r.id ASC""",
            (case, metric)).fetchall()]
        direction = con.execute(
            "SELECT direction FROM metrics WHERE case_name = ? AND metric = ? LIMIT 1",
            (case, metric)).fetchone()[0]
        result = check_history(values, direction, **kwargs)
        if result is None:
            out.append((case, metric, "insufficient_samples", None))
        else:
            regressed, detail = result
            out.append((case, metric, "regression" if regressed else "ok", detail))
    return out
