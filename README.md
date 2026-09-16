# Nairobi Air Quality Batch Pipeline

A batch data pipeline for collecting Nairobi air quality readings from the OpenAQ API, normalizing them, validating the payloads, and loading them into PostgreSQL for downstream analysis.

## Overview

This project uses Apache Airflow to orchestrate a daily ETL workflow:

1. Query the OpenAQ API for Nairobi-area locations.
2. Discover the sensors associated with each location.
3. Fetch recent measurements for each sensor.
4. Normalize and validate the records.
5. Upsert the cleaned results into Postgres.

The DAG is defined in [dags/nairobi_air_quality_dag.py](dags/nairobi_air_quality_dag.py) and the API client lives in [src/nairobi_air_quality_batch_pipeline/api/openaq_client.py](src/nairobi_air_quality_batch_pipeline/api/openaq_client.py).

## Project structure

- [dags/nairobi_air_quality_dag.py](dags/nairobi_air_quality_dag.py) — Airflow DAG that orchestrates the workflow
- [src/nairobi_air_quality_batch_pipeline/api/openaq_client.py](src/nairobi_air_quality_batch_pipeline/api/openaq_client.py) — OpenAQ API client wrapper
- [sql/create_tables.sql](sql/create_tables.sql) — database schema for raw measurements
- [docker-compose.yml](docker-compose.yml) — local PostgreSQL and app services
- [Dockerfile](Dockerfile) — container image for the pipeline environment
- [tests/](tests/) — basic smoke tests for import and Docker asset setup

```text
nairobi_air_quality_batch_pipeline/
├── dags/
│   └── nairobi_air_quality_dag.py
├── src/
│   └── nairobi_air_quality_batch_pipeline/
│       ├── __init__.py
│       └── api/
│           ├── __init__.py
│           └── openaq_client.py
├── sql/
│   └── create_tables.sql
├── tests/
│   ├── test_dag_import_order.py
│   └── test_docker_files.py
├── .env.example
├── .dockerignore
├── .gitignore
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── README.md
├── logs/
└── config/
```

## Stack

- Python 3.12+
- Apache Airflow 3.3.1
- PostgreSQL 16
- Docker and Docker Compose
- OpenAQ API key configured in Airflow connections
- uv

## Prerequisites

- Docker Desktop or Docker Engine with Compose
- A valid OpenAQ API key, if you are using the authenticated API
- Optional: Python 3.12+ and `uv` if you want to run the project locally without Docker

## Local setup

### 1) Clone the repository

```bash
git clone <your-repository-url>
cd nairobi_air_quality_batch_pipeline
```

### 2) Configure environment variables

Create a `.env` file in the project root with values such as:

```env
OPENAQ_HOST=api.openaq.org
OPENAQ_API_KEY=your_api_key_here
```

### 3) Start the services

```bash
docker compose up --build
```

### 4) Open Airflow UI in browser
```bash
http://localhost:8080
```

Default Airflow login:
- Username: `admin`
- Password: `admin`

This starts:
- PostgreSQL on port `5432`
- Airflow webserver
- Airflow scheduler
- the application container with the project source mounted for development

## Database schema

The pipeline writes measurements into the `raw_air_quality` table defined in [sql/create_tables.sql](sql/create_tables.sql):

- `sensor_id`
- `measurement_timestamp`
- `location_id`
- `location_name`
- `parameter`
- `unit`
- `value`

The table enforces a primary key on `(sensor_id, measurement_timestamp)` and uses an `ON CONFLICT ... DO UPDATE` pattern to keep the data idempotent.


## DAG behavior

The DAG runs daily and performs the following tasks:
- `extract_locations()` — fetches Nairobi-area locations from OpenAQ
- `extract_sensors()` — fetches sensors per location
- `flatten_sensors()` — flattens nested sensor lists
- `extract_measurements()` — pulls recent measurements for each sensor
- `flatten_measurements()` — flattens nested measurement results
- `validate_measurements()` — removes invalid or incomplete records
- `load_measurements()` — inserts or updates records in PostgreSQL


## Testing

The project includes lightweight tests to verify import ordering and Docker assets:

```bash
uv rub pytest
```

## Notes

- The DAG is configured to run on a daily schedule with retry logic.
- The pipeline intentionally validates fields like sensor IDs, timestamps, parameter names, units, and numeric values before insertion.
- Credentials and runtime settings should be kept in environment variables rather than hardcoded in source files.

## License

This project is intended for internal or personal use unless otherwise specified by the repository owner.
