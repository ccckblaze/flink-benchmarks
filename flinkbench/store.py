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
"""Local, self-contained result store — the Codespeed-free replacement.

One sqlite file holds every measured value: a `runs` row per captured source
(a rig log, a JMH CSV, a manual registration) and `metrics` rows keyed by
(case, metric) with unit and direction, so reports and regression checks read
exactly the history upstream flink-benchmarks keeps on its Codespeed server.
"""

import os
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    host TEXT NOT NULL,
    suite TEXT NOT NULL,
    source TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS metrics (
    run_id INTEGER NOT NULL REFERENCES runs(id),
    case_name TEXT NOT NULL,
    metric TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('lower_is_better', 'higher_is_better'))
);
CREATE INDEX IF NOT EXISTS metrics_key ON metrics (case_name, metric);
"""


def connect(path):
    d = os.path.dirname(os.path.abspath(path))
    if d:
        os.makedirs(d, exist_ok=True)
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    return con


def add_run(con, host, suite, source):
    cur = con.execute(
        "INSERT INTO runs (ts, host, suite, source) VALUES (datetime('now'), ?, ?, ?)",
        (host, suite, source),
    )
    return cur.lastrowid


def add_metric(con, run_id, case, metric, value, unit, direction):
    con.execute(
        "INSERT INTO metrics (run_id, case_name, metric, value, unit, direction) VALUES (?, ?, ?, ?, ?, ?)",
        (run_id, case, metric, float(value), unit, direction),
    )


def latest_run_id(con):
    row = con.execute("SELECT max(id) FROM runs").fetchone()
    return row[0]


def rows_of_run(con, run_id):
    return con.execute(
        "SELECT case_name, metric, value, unit, direction FROM metrics WHERE run_id = ?", (run_id,)
    ).fetchall()


def history(con, case, metric):
    """Oldest-first (ts, value) pairs for the regression window."""
    return con.execute(
        """SELECT r.ts, m.value FROM metrics m JOIN runs r ON r.id = m.run_id
           WHERE m.case_name = ? AND m.metric = ? ORDER BY r.ts ASC, r.id ASC""",
        (case, metric),
    ).fetchall()


def keys(con):
    return con.execute("SELECT DISTINCT case_name, metric FROM metrics ORDER BY case_name, metric").fetchall()
