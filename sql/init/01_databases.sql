SELECT format('CREATE DATABASE airflow OWNER %I', current_user)
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')
\gexec

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.weather_daily (
    city_id text NOT NULL,
    city_name text NOT NULL,
    logical_date date NOT NULL,
    payload jsonb NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (city_id, logical_date)
);