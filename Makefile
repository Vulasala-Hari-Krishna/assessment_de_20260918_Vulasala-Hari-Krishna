.PHONY: up down logs airflow-ui notebook dbt dbt-test dbt-docs test psql reproduce

up:
	docker compose up -d --build --wait --wait-timeout 180

down:
	docker compose down

logs:
	docker compose logs -f airflow

airflow-ui:
	@echo "Airflow:    http://localhost:8080  (admin / admin)"

notebook:
	@echo "JupyterLab: http://localhost:8888  (open notebooks/walkthrough.ipynb)"

dbt:
	docker compose exec -T airflow bash -c "cd /opt/airflow/dbt && dbt run"

dbt-test:
	docker compose exec -T airflow bash -c "cd /opt/airflow/dbt && dbt test"

dbt-docs:
	docker compose exec -T airflow bash -c "cd /opt/airflow/dbt && dbt docs generate"

test:
	docker compose exec -T airflow python -m unittest discover -s /opt/airflow/ingestion/tests -v

psql:
	docker compose exec postgres psql -U $${POSTGRES_USER:-de} -d $${POSTGRES_DB:-warehouse}

# Executes the walkthrough top to bottom with a fresh kernel and writes the result next to it.
# Reviewers run this on a clean checkout and diff it against the committed notebook.
reproduce:
	docker compose exec -T jupyter jupyter nbconvert --to notebook --execute \
		--ExecutePreprocessor.timeout=1800 \
		--output walkthrough.reproduced.ipynb /opt/airflow/notebooks/walkthrough.ipynb
	@echo "Wrote notebooks/walkthrough.reproduced.ipynb"
