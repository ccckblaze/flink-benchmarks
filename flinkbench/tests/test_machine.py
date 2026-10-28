# -*- coding: utf-8 -*-
"""Profile rendering must survive a partially measured machine capture."""

import importlib.util
import os
import unittest

from flinkbench import report
from flinkbench.chart import _keyed_machine_note

FIXTURE = {
    "generated_utc": "2026-10-28T13:11:51Z",
    "hardware": {
        "model": "General Processors from AMD",
        "arch": "x86_64", "hypervisor": "KVM", "virtualization_type": "full",
        "sockets": "1", "threads_per_core": "2", "nproc": "4",
        "caches": {"L1d": "64 KiB (2 instances)", "L3": "32 MiB (1 instance)"},
        "flags": {"avx2": "yes", "avx512f": "no"},
        "mem_total_mb": 15480, "mem_available_mb": 14563.9,
        "thp": "always [madvise] never", "nic": "eth0=speed:-1",
        "block_devices": [{"name": "vdb", "size_bytes": 42949672960,
                           "type": "disk", "transport": "virtio"}],
    },
    "disk": {"datadisk": {"seq_write_mibps": 224.0, "seq_read_mibps": 210.0,
                          "randread_kiops": 4.569, "fsync_p50_ms": 0.272,
                          "fsync_p99_ms": 1.286, "write_mode": "direct",
                          "read_mode": "cached_fallback"},
             "root": {"seq_write_mibps": 240.0}},
    "memory": {"copy1_gibps": 14.0, "triad1_gibps": 14.5,
               "copyall_gibps": 29.3, "copyall_threads": 4},
}


class MachineRowsTest(unittest.TestCase):
    def test_full_capture_renders_all_three_tables(self):
        rows = report._machine_rows(FIXTURE)
        text = "\n".join(rows)
        self.assertIn("### 机器画像", text)
        self.assertIn("### 磁盘基线", text)
        self.assertIn("### 内存带宽", text)
        self.assertIn("0.27 ms", text)
        self.assertIn("KVM 虚拟机", text)
        self.assertIn("29.3 GiB/s", text)
        self.assertIn("cached_fallback", text)  # read mode caveat surfaces

    def test_partial_capture_renders_dashes(self):
        rows = report._machine_rows({"hardware": {}, "disk": {}, "memory": {}})
        text = "\n".join(rows)
        self.assertIn("### 磁盘基线", text)
        self.assertIn("—", text)

    def test_no_memory_block_skips_bandwidth_table(self):
        rows = report._machine_rows({"hardware": {}, "disk": {}, "memory": {}})
        self.assertNotIn("### 内存带宽", "\n".join(rows))

    def test_machine_note_uses_profile_cache_size(self):
        note = _keyed_machine_note(FIXTURE)
        self.assertIn("32 MiB", note)
        self.assertIn("KVM", note)


class EnvSnapshotCacheTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = os.path.join(os.path.dirname(report.__file__), "..", "fabperf",
                            "env_snapshot.py")
        spec = importlib.util.spec_from_file_location("env_snapshot",
                                                      os.path.abspath(path))
        cls.mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.mod)

    def test_cache_bytes_parses_lscpu_shape(self):
        caches = {"L3": "32 MiB (1 instance)", "L2": "8 MiB (2 instances)"}
        self.assertEqual(self.mod.cache_bytes(caches, "L3"), 32 * 1024 * 1024)

    def test_cache_bytes_defends_against_missing(self):
        self.assertIsNone(self.mod.cache_bytes({}, "L3"))
        self.assertIsNone(self.mod.cache_bytes({"L3": "not a size"}, "L3"))


if __name__ == "__main__":
    unittest.main()
