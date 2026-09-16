FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src \
    AIRFLOW_HOME=/opt/airflow

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY dags ./dags
COPY sql ./sql
COPY tests ./tests

RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install . \
    && python -m pip install pytest

CMD ["python", "-c", "import nairobi_air_quality_batch_pipeline; print('nairobi_air_quality_batch_pipeline image ready')"]
