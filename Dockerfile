FROM apache/airflow:2.7.1

WORKDIR /opt/airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY workflows /opt/airflow/dags
COPY scripts /opt/airflow/scripts
COPY dataset /opt/airflow/dataset
COPY sql /opt/airflow/sql

ENV PYTHONPATH="/opt/airflow:/opt/airflow/scripts"
