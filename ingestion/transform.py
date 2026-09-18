"""Run the same checked dbt commands in Airflow and in the notebook."""

import os
import subprocess

from ingestion.pipeline import PROJECT_ROOT


def run_dbt(command):
    commands = {"run": ["run"], "test": ["test"], "docs": ["docs", "generate"]}
    if command not in commands:
        raise ValueError(f"Unsupported dbt command: {command}")
    result = subprocess.run(
        ["dbt", "--no-use-colors", *commands[command],
         "--project-dir", str(PROJECT_ROOT / "dbt"),
         "--profiles-dir", os.environ.get("DBT_PROFILES_DIR", str(PROJECT_ROOT / "dbt"))],
        cwd=PROJECT_ROOT / "dbt",
        text=True,
        capture_output=True,
        timeout=600,
        check=False,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    result.check_returncode()
    return {"command": command, "returncode": result.returncode}