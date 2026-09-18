# Notes

## Time spent

Roughly how many hours, and how it was split (setup / extract-load / dbt / airflow / notebook).
- Total time : 4 hours
- understanding the task: 30 mins
- setup : 45 mins
- extract-load : 30 mins
- dbt : 60 mins
- airflow : 30 mins
- notebook : 40 mins

## What I would do with more time

- **Improve monitoring, Alerts, CI/CD:** Add alerts for failed runs, missing city/date combinations
	and stale data. Move the notebook's completeness checks into automated pipeline checks.
- **Strengthen failure testing:** Add integration tests for API timeouts, rate limits,
	database failures, transaction rollback and concurrent loads.
- **Validate before publishing:** Build and test a candidate mart before replacing the
	reporting table, preserving the last valid dataset when quality checks fail.
- **Improve deployment security:** Replace demo credentials, enable Jupyter
	authentication and use a least-privilege database account before any non-local deployment.

## Known gaps

- **Recent archive availability:** Yesterday's data may be incomplete or unavailable.
	The pipeline fails explicitly rather than inventing values, but retries cannot
	guarantee availability.
- **No revision history:** Raw storage retains the latest response per city/date.
	Previous versions are overwritten, and older dates are not automatically refreshed.
- **Limited concurrency coordination:** Airflow serializes its DAG runs, but notebook
	execution is independent. Running both simultaneously can cause competing mart rebuilds.
- **Tests run after publication:** A failing dbt test marks execution as failed, but
	does not automatically restore the previous mart.
- **Small-scale validation:** Testing covered the three configured cities, a 30-day
	window and an explicit Airflow backfill. Long-running daily operation and larger
	workloads have not been validated.
- **Local demonstration setup:** Default credentials and token-free Jupyter are
	suitable only for the localhost-bound assessment environment, not a production deployment.


## AI-usage declaration

Be specific. Examples of acceptable use: "asked ChatGPT how to configure dbt profiles for
Postgres", "used Copilot for boilerplate in the API client". Examples of unacceptable
use: "generated the DAG and dbt models from the brief".

- Used Copilot to look up API docs, schema related stuff. Adding function, module docstring. 
- Finding syntax or built-in functions for dbt-core, dbt-postgres, airflow libraries. docker yaml syntax.

| Where (file / area)                                           | What the tool did                            | What I changed afterwards                    |
|---------------------------------------------------------------|----------------------------------------------|----------------------------------------------|
| API schema details(extraction-load)                           | provided open-meteo schema, how to use, docs | Verified                                     |
| Docker yaml syntax(docker-compose.yml,Makefile)               | Helped with environment setup                | Verified                                     |
| Documentation, function docstrings, module doc in All scripts | Helped with documentation                    | Verified                                     |
| dbd-core library syntax                                       | Helped with dbt-core library syntax          | Verified and used it to complete the task    |
| Airflow syntax                                                | helped with airflow syntax to build DAG      | Verified and used it for completing the task |
