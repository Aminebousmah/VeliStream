FROM python:3.12-slim

RUN pip install --no-cache-dir uv

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY producer ./producer
COPY orchestration ./orchestration
COPY dbt ./dbt
RUN uv sync --frozen --no-dev && mkdir -p .dagster
