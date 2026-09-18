from datetime import timedelta
import subprocess
import unittest
from unittest.mock import Mock, patch

from airflow.models import DagBag

from ingestion.pipeline import PROJECT_ROOT, extract_date, load_date
from ingestion.transform import run_dbt


class WorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(dag_folder=str(PROJECT_ROOT / "dags"), include_examples=False)

    def test_dag_imports_and_uses_shared_functions_in_order(self):
        self.assertEqual(self.bag.import_errors, {})
        dag = self.bag.get_dag("weather_daily")
        self.assertEqual(set(dag.task_ids), {"extract", "load", "dbt_run", "dbt_test"})
        self.assertIs(dag.get_task("extract").python_callable, extract_date)
        self.assertIs(dag.get_task("load").python_callable, load_date)
        self.assertIs(dag.get_task("dbt_run").python_callable, run_dbt)
        self.assertIs(dag.get_task("dbt_test").python_callable, run_dbt)
        for before, after in (("extract", "load"), ("load", "dbt_run"), ("dbt_run", "dbt_test")):
            self.assertEqual(dag.get_task(before).downstream_task_ids, {after})
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        for task in dag.tasks:
            self.assertEqual(task.retries, 2)
            self.assertEqual(task.execution_timeout, timedelta(minutes=15))

    def test_templates_use_logical_date_and_native_records(self):
        dag = self.bag.get_dag("weather_daily")
        task_instance = Mock()
        records = [{"logical_date": "2026-01-15", "payload": {"daily": {}}}]
        task_instance.xcom_pull.return_value = records
        for task_id in ("extract", "load"):
            task = dag.get_task(task_id)
            task.render_template_fields({"ds": "2026-01-15", "ti": task_instance})
            self.assertEqual(task.op_kwargs["logical_date"], "2026-01-15")
        self.assertEqual(dag.get_task("load").op_kwargs["records"], records)

    @patch("ingestion.transform.subprocess.run")
    def test_dbt_failure_is_not_hidden(self, run):
        run.return_value = subprocess.CompletedProcess(["dbt"], 1, "test failed", "")
        with self.assertRaises(subprocess.CalledProcessError):
            run_dbt("test")
        self.assertEqual(run.call_args.kwargs["timeout"], 600)

    @patch("ingestion.transform.subprocess.run")
    def test_dbt_uses_project_profile(self, run):
        run.return_value = subprocess.CompletedProcess(["dbt"], 0, "success", "")
        self.assertEqual(run_dbt("run"), {"command": "run", "returncode": 0})
        self.assertIn(str(PROJECT_ROOT / "dbt"), run.call_args.args[0])

    def test_dbt_rejects_unknown_commands(self):
        with self.assertRaises(ValueError):
            run_dbt("shell")


if __name__ == "__main__":
    unittest.main()