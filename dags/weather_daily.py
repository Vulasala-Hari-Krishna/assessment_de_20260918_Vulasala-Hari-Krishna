"""Daily, date-addressable weather ingestion followed by tested dbt models."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from ingestion.pipeline import extract_date, load_date
from ingestion.transform import run_dbt


with DAG(
    dag_id="weather_daily",
    description="Open-Meteo raw weather to tested city/day reporting",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    render_template_as_native_obj=True,
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=20),
        "execution_timeout": timedelta(minutes=15),
    },
    tags=["weather", "assessment"],
) as dag:
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_date,
        op_kwargs={"logical_date": "{{ ds }}"},
        show_return_value_in_logs=False,
    )
    load = PythonOperator(
        task_id="load",
        python_callable=load_date,
        op_kwargs={
            "logical_date": "{{ ds }}",
            "records": "{{ ti.xcom_pull(task_ids='extract') }}",
        },
    )
    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt,
        op_kwargs={"command": "run"},
        do_xcom_push=False,
    )
    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt,
        op_kwargs={"command": "test"},
        do_xcom_push=False,
    )

    extract >> load >> dbt_run >> dbt_test