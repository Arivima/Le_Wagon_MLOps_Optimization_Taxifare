FROM python:3.8.14-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED 1

ENV AIRFLOW_HOME=/app/airflow

RUN apt-get update && apt-get install -y curl \
    gcc default-mysql-server default-libmysqlclient-dev

WORKDIR $AIRFLOW_HOME

COPY . .
RUN chmod +x scripts/entrypoint.sh

RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install --without dev \
    && poetry build --format wheel
