"""Offline contract tests; these do not replace a real Spark/Prefect cluster run."""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        rows = [dict(event_date="2026-10-20", events_count=3, unique_users=2,
                     page_views=1, signups=1, purchases=1, purchase_share=0.333333),
                dict(event_date="2026-10-21", events_count=2, unique_users=2,
                     page_views=1, signups=1, purchases=0, purchase_share=0.0)]
        self.metrics = dict(master="yarn", application_id="application_1_0001",
                            input_uri="hdfs://nn:9000/input", output_uri="hdfs://nn:9000/output",
                            input_rows=5, valid_rows=5, output_rows=2, clean_rows=5, rows=rows)
        self.write("metrics.json", json.dumps(self.metrics))
        self.write("yarn-application.log", "Application-Type : SPARK\nFinal-State : SUCCEEDED\n")
        self.tsv = "2026-10-20\t3\t2\t1\t1\t1\t0.333333\n2026-10-21\t2\t2\t1\t1\t0\t0.0\n"
        self.write("hive-rows.tsv", self.tsv)

    def write(self, name, content):
        (self.path / name).write_text(content, encoding="utf-8")

    def run_script(self, script, stdin=None):
        return subprocess.run([sys.executable, str(ROOT / "scripts" / script), str(self.path)],
                              input=stdin, text=True, capture_output=True)

    def test_matching_hive_rows(self):
        result = self.run_script("verify.py")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(result.stdout)["hive_matches_spark"])

    def test_corrupted_aggregate_rejected(self):
        self.write("hive-rows.tsv", self.tsv.replace("\t3\t2", "\t4\t2"))
        self.assertNotEqual(self.run_script("verify.py").returncode, 0)

    def test_missing_partition_rejected(self):
        self.write("hive-rows.tsv", self.tsv.splitlines()[0] + "\n")
        self.assertNotEqual(self.run_script("verify.py").returncode, 0)

    def test_duplicate_partition_rejected(self):
        self.write("hive-rows.tsv", self.tsv + self.tsv)
        self.assertNotEqual(self.run_script("verify.py").returncode, 0)

    def test_local_spark_rejected(self):
        self.metrics["master"] = "local[*]"
        self.write("metrics.json", json.dumps(self.metrics))
        self.assertNotEqual(self.run_script("verify.py").returncode, 0)

    def test_failed_yarn_rejected_even_with_success_in_diagnostics(self):
        self.write("yarn-application.log", "Application-Type : SPARK\nFinal-State : FAILED\nDiagnostics : SUCCEEDED earlier\n")
        self.assertNotEqual(self.run_script("verify.py").returncode, 0)

    def test_metrics_extraction(self):
        self.write("spark.log", "Spark startup\nHW5_METRICS=" + json.dumps(self.metrics) + "\n")
        self.assertEqual(self.run_script("extract-metrics.py").returncode, 0)
        self.assertEqual(json.loads((self.path / "metrics.json").read_text()), self.metrics)

    def test_missing_and_duplicate_metrics_rejected(self):
        for text in ["no metrics", ("HW5_METRICS={}\n" * 2)]:
            self.write("spark.log", text)
            self.assertNotEqual(self.run_script("extract-metrics.py").returncode, 0)

    def test_executor_and_successful_task_evidence(self):
        events = [dict(Event="SparkListenerExecutorAdded", **{
            "Executor ID": "1", "Executor Info": {"Host": "worker"}}),
                  dict(Event="SparkListenerTaskEnd", **{
                      "Task End Reason": {"Reason": "Success"}, "Task Info": {"Host": "worker"}})]
        result = self.run_script("inspect-event-log.py", "\n".join(map(json.dumps, events)))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["successful_tasks_by_host"], {"worker": 1})
        self.assertNotEqual(self.run_script("inspect-event-log.py", "").returncode, 0)

    def prepare_report_inputs(self):
        result = self.run_script("verify.py")
        self.assertEqual(result.returncode, 0, result.stderr)
        verification = json.loads(result.stdout)
        self.write("verification.json", json.dumps(verification))
        self.write("flow-result.json", json.dumps({"application_id": self.metrics["application_id"],
                                                   "verification": verification}))
        self.write("executors.json", json.dumps({"executors": {"1": "worker"},
                                                 "successful_tasks_by_host": {"worker": 2}}))
        for name in ["spark.log", "prefect.log", "hive-register.log"]:
            self.write(name, "synthetic fixture\n")

    def run_report(self):
        return subprocess.run([sys.executable, str(ROOT / "scripts/collect-evidence.py"),
                               str(self.path), "--output", str(self.path / "report.md")],
                              capture_output=True, text=True)

    def test_report_from_complete_run(self):
        self.prepare_report_inputs()
        result = self.run_report()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = (self.path / "report.md").read_text(encoding="utf-8")
        self.assertIn("application_1_0001", report)
        self.assertIn("2026-10-20 | 3 | 2 | 1 | 1 | 1", report)

    def test_incomplete_run_cannot_generate_report(self):
        self.assertNotEqual(self.run_report().returncode, 0)
        self.assertFalse((self.path / "report.md").exists())

    def test_mixed_application_ids_rejected(self):
        self.prepare_report_inputs()
        result = json.loads((self.path / "flow-result.json").read_text())
        result["application_id"] = "application_1_9999"
        self.write("flow-result.json", json.dumps(result))
        self.assertNotEqual(self.run_report().returncode, 0)
        self.assertFalse((self.path / "report.md").exists())

    def test_failed_run_preserves_previous_report(self):
        self.write("report.md", "previous successful report")
        self.assertNotEqual(self.run_report().returncode, 0)
        self.assertEqual((self.path / "report.md").read_text(), "previous successful report")


if __name__ == "__main__":
    unittest.main()
