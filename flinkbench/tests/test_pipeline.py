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
"""Ingestion and regression unit tests (no rig access needed)."""

import unittest

from flinkbench import gate, ingest, regression, store

JMH_CSV = """Benchmark,Mode,Fork,Java,VM,Threads,Extra,Param,Param,Score,Score Error (99.9999%),Unit
org.apache.flink.benchmark.WordCountBenchmark.test,thrpt,1,17,server,4,,ROCKSDB,20,12345.678,100.5,ops/s
org.apache.flink.benchmark.state.ValueStateBenchmark.updateAverage,avgt,1,17,server,4,,,,"67.89",1.5,ms/op
"""

MEASURED = """
test framing_encodes_and_decodes_at_streaming_throughput_measured ... MEASURED encode: 33.1 MiB in 19.310ms -> 1716.0 MiB/s, 18.42 ns/row
MEASURED decode: 33.1 MiB in 17.092ms -> 1938.6 MiB/s, 16.30 ns/row, 4.075 ns/cell
MEASURED wire: 33.136 bytes/row
MEASURED take: 2048 x 8192 rows in 51.128185ms -> 328.1 Mrows/s, 3.05 ns/row (4 cols: 0.762 ns/cell)
M98 curve | batch=   16 n= 7680 wire=   9.8 MiB | L2 wrap     4323 ns/batch   270.2 ns/row | L1 IPC     16998 ns/batch  1062.4 ns/row | L1/L2 transfer ratio 3.93x
P57-TABLE|int+long|rust=5|jvm=13|jvm_over_rust=2.60x|records=2000000
P57-FUSE chained=120 ns/rec fused=77 ns/rec speedup=1.55x records=2000000
"""


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.con = store.connect(":memory:")
        self.run = store.add_run(self.con, "unit", "test", "fixture")

    def test_jmh_csv_ingest(self):
        n = ingest.ingest_jmh_csv(self.con, self.run, JMH_CSV)
        self.assertEqual(n, 2)
        rows = {(c, m): v for (c, m, v, _, _) in store.rows_of_run(self.con, self.run)}
        dirs = {(c, m): d for (c, m, _, _, d) in store.rows_of_run(self.con, self.run)}
        self.assertAlmostEqual(rows[("test.ROCKSDB.20", "score")], 12345.678)
        self.assertAlmostEqual(rows[("test.ROCKSDB.20", "error")], 100.5)
        self.assertAlmostEqual(rows[("updateAverage", "score")], 67.89)
        self.assertEqual(dirs[("test.ROCKSDB.20", "score")], "higher_is_better")
        self.assertEqual(dirs[("updateAverage", "score")], "lower_is_better")

    def test_jmh_csv_default_confidence_error_kept(self):
        # JMH writes "Score Error (99.9%)" unless a confidence was requested;
        # the error bar must survive ingestion either way.
        csv_text = ('Benchmark,Mode,Threads,Samples,Score,Score Error (99.9%),Unit\n'
                    'org.apache.flink.benchmark.WindowBenchmarks.globalWindow,'
                    'thrpt,1,7,5478.578,154.666,ops/ms\n')
        n = ingest.ingest_jmh_csv(self.con, "jmh-conf-run", csv_text)
        self.assertEqual(n, 1)
        rows = {(c, m): v for (c, m, v, _, _) in store.rows_of_run(self.con, "jmh-conf-run")}
        self.assertAlmostEqual(rows[("globalWindow", "score")], 5478.578)
        self.assertAlmostEqual(rows[("globalWindow", "error")], 154.666)

    def test_measured_ingest(self):
        n = ingest.ingest_measured_log(self.con, self.run, MEASURED)
        rows = {(c, m): v for (c, m, v, _, _) in store.rows_of_run(self.con, self.run)}
        self.assertGreaterEqual(n, 12)
        self.assertAlmostEqual(rows[("framing_encode", "throughput")], 1716.0)
        self.assertAlmostEqual(rows[("serde_int_long", "flinkable")], 5.0)
        self.assertAlmostEqual(rows[("serde_int_long", "flink_jvm")], 13.0)
        self.assertAlmostEqual(rows[("smallbatch_16", "transfer_ratio")], 3.93)
        self.assertAlmostEqual(rows[("operator_fusion", "speedup")], 1.55)


class RegressionTest(unittest.TestCase):
    def test_throughput_regression_detected(self):
        hist = [1000, 1010, 995, 1005, 1000, 1002, 700, 710, 690]
        regressed, detail = regression.check_history(hist, "higher_is_better")
        self.assertTrue(regressed)
        self.assertGreater(detail["regression_ratio"], 0.25)

    def test_stable_throughput_ok(self):
        hist = [1000, 1010, 995, 1005, 1000, 1002, 999, 1001]
        regressed, _ = regression.check_history(hist, "higher_is_better")
        self.assertFalse(regressed)

    def test_latency_regression_direction(self):
        hist = [10, 11, 9, 10, 12, 10, 30, 31, 29]
        regressed, _ = regression.check_history(hist, "lower_is_better")
        self.assertTrue(regressed)

    def test_short_history_returns_none(self):
        self.assertIsNone(regression.check_history([1, 2, 3], "higher_is_better"))

    def test_instability_gate_suppresses_noise(self):
        hist = [1000, 100, 1900, 100, 1900, 100, 1900, 100]
        regressed, _ = regression.check_history(hist, "higher_is_better", instability_multiplier=2.0)
        self.assertFalse(regressed)


if __name__ == "__main__":
    unittest.main()



CANON_IN = "P57-TABLE|shape-mixed|rust=132|jvm=160|jvm_over_rust=1.21x|records=2000000\n"
CANON_DRIFT = "P57-TABLE|shape-mixed|rust=132|jvm=118|jvm_over_rust=0.89x|records=2000000\n"


class GateTest(unittest.TestCase):
    """The JVM-band gate: recent-regime median, session excluded, drift refused."""

    def setUp(self):
        self.con = store.connect(":memory:")
        for source, value in (("S1", 162.0), ("S2", 160.0), ("S3", 161.0)):
            run = store.add_run(self.con, "unit", "native", source)
            store.add_metric(self.con, run, "serde_shape_mixed", "flink_jvm", value, "ns/rec", "lower_is_better")

    def test_in_band_session_passes(self):
        ok, rows = gate.check(self.con, CANON_IN, exclude_source="S4")
        self.assertTrue(ok)
        self.assertEqual(rows[0][4], "ok")

    def test_drifted_leg_fails_the_gate(self):
        ok, rows = gate.check(self.con, CANON_DRIFT, exclude_source="S4")
        self.assertFalse(ok)
        self.assertEqual(rows[0][4], "drift")

    def test_session_under_review_does_not_anchor_itself(self):
        run = store.add_run(self.con, "unit", "native", "S4")
        store.add_metric(self.con, run, "serde_shape_mixed", "flink_jvm", 118.0, "ns/rec", "lower_is_better")
        ok, _rows = gate.check(self.con, CANON_DRIFT, exclude_source="S4")
        self.assertFalse(ok, "the drifted session's own stored row must not join the band")

    def test_old_regime_does_not_vote(self):
        # A harness re-anchor moves the level; the band is the recent window only.
        old = store.add_run(self.con, "unit", "native", "S0")
        store.add_metric(self.con, old, "serde_shape_mixed", "flink_jvm", 108.0, "ns/rec", "lower_is_better")
        ok, rows = gate.check(self.con, CANON_IN, exclude_source="S4")
        self.assertTrue(ok)
        self.assertGreater(rows[0][2], 150.0, "median must sit in the recent regime")
class RegressionTest(unittest.TestCase):
    def test_throughput_regression_detected(self):
        hist = [1000, 1010, 995, 1005, 1000, 1002, 700, 710, 690]
        regressed, detail = regression.check_history(hist, "higher_is_better")
        self.assertTrue(regressed)
        self.assertGreater(detail["regression_ratio"], 0.25)

    def test_stable_throughput_ok(self):
        hist = [1000, 1010, 995, 1005, 1000, 1002, 999, 1001]
        regressed, _ = regression.check_history(hist, "higher_is_better")
        self.assertFalse(regressed)

    def test_latency_regression_direction(self):
        hist = [10, 11, 9, 10, 12, 10, 30, 31, 29]
        regressed, _ = regression.check_history(hist, "lower_is_better")
        self.assertTrue(regressed)

    def test_short_history_returns_none(self):
        self.assertIsNone(regression.check_history([1, 2, 3], "higher_is_better"))

    def test_instability_gate_suppresses_noise(self):
        hist = [1000, 100, 1900, 100, 1900, 100, 1900, 100]
        regressed, _ = regression.check_history(hist, "higher_is_better", instability_multiplier=2.0)
        self.assertFalse(regressed)


if __name__ == "__main__":
    unittest.main()


CANON_IN = "P57-TABLE|shape-mixed|rust=132|jvm=160|jvm_over_rust=1.21x|records=2000000\n"
CANON_DRIFT = "P57-TABLE|shape-mixed|rust=132|jvm=118|jvm_over_rust=0.89x|records=2000000\n"


class GateTest(unittest.TestCase):
    """The JVM-band gate: recent-regime median, session excluded, drift refused."""

    def setUp(self):
        self.con = store.connect(":memory:")
        for source, value in (("S1", 162.0), ("S2", 160.0), ("S3", 161.0)):
            run = store.add_run(self.con, "unit", "native", source)
            store.add_metric(self.con, run, "serde_shape_mixed", "flink_jvm", value, "ns/rec", "lower_is_better")

    def test_in_band_session_passes(self):
        ok, rows = gate.check(self.con, CANON_IN, exclude_source="S4")
        self.assertTrue(ok)
        self.assertEqual(rows[0][4], "ok")

    def test_drifted_leg_fails_the_gate(self):
        ok, rows = gate.check(self.con, CANON_DRIFT, exclude_source="S4")
        self.assertFalse(ok)
        self.assertEqual(rows[0][4], "drift")

    def test_session_under_review_does_not_anchor_itself(self):
        run = store.add_run(self.con, "unit", "native", "S4")
        store.add_metric(self.con, run, "serde_shape_mixed", "flink_jvm", 118.0, "ns/rec", "lower_is_better")
        ok, _rows = gate.check(self.con, CANON_DRIFT, exclude_source="S4")
        self.assertFalse(ok, "the drifted session's own stored row must not join the band")

    def test_old_regime_does_not_vote(self):
        # A harness re-anchor moves the level; the band is the recent window only.
        old = store.add_run(self.con, "unit", "native", "S0")
        store.add_metric(self.con, old, "serde_shape_mixed", "flink_jvm", 108.0, "ns/rec", "lower_is_better")
        ok, rows = gate.check(self.con, CANON_IN, exclude_source="S4")
        self.assertTrue(ok)
        self.assertGreater(rows[0][2], 150.0, "median must sit in the recent regime")


