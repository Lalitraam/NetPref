import os
import sys
import unittest
from pathlib import Path

import config


class TestCrossPlatformConfig(unittest.TestCase):

    def test_paths_exist_or_valid(self):
        self.assertIsInstance(config.ROOT_DIR, Path)
        self.assertIsInstance(config.DATA_DIR, Path)
        self.assertIsInstance(config.MODELS_DIR, Path)
        self.assertIsInstance(config.OUTPUT_DIR, Path)
        self.assertIsInstance(config.SCORED_FLOWS_DIR, Path)
        self.assertIsInstance(config.WAREHOUSE_DB, Path)
        self.assertIsInstance(config.HBASE_DB, Path)
        self.assertTrue(config.ROOT_DIR.exists())

    def test_kafka_defaults(self):
        self.assertEqual(config.KAFKA_TOPIC, "flows")
        self.assertIn("localhost", config.KAFKA_BOOTSTRAP_SERVERS)

    def test_kafka_logs_dir_resolution(self):
        resolved = config.get_kafka_logs_dir()
        self.assertIsInstance(resolved, Path)

        # Test env override
        os.environ["KAFKA_LOGS_DIR"] = "/tmp/test_kafka_logs_custom"
        self.assertEqual(config.get_kafka_logs_dir(), Path("/tmp/test_kafka_logs_custom"))
        del os.environ["KAFKA_LOGS_DIR"]

    def test_spark_environment_setup(self):
        config.setup_spark_environment()
        self.assertEqual(os.environ.get("PYSPARK_PYTHON"), sys.executable)
        self.assertEqual(os.environ.get("PYSPARK_DRIVER_PYTHON"), sys.executable)

    def test_schema_definition(self):
        self.assertIn("flow_id", config.STREAMING_SCHEMA)
        self.assertIn("degraded", config.STREAMING_SCHEMA)
        self.assertIn("link_id", config.STREAMING_SCHEMA)
        self.assertIn("event_ts_ms", config.STREAMING_SCHEMA)

    def test_reversed_timestamp_ordering(self):
        # Verify that newer timestamps produce lexicographically smaller strings
        max_ts = 9_999_999_999_999
        t_older = 1700000000000
        t_newer = 1700000010000

        r_older = f"{max_ts - t_older:013d}"
        r_newer = f"{max_ts - t_newer:013d}"

        self.assertLess(r_newer, r_older, "Newer timestamp must sort before older timestamp")


if __name__ == "__main__":
    unittest.main()
