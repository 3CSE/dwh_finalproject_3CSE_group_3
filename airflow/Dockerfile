FROM apache/airflow:2.7.1

WORKDIR /opt/airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGs and scripts
COPY workflows /opt/airflow/dags
COPY scripts /opt/airflow/scripts

# Set PYTHONPATH
ENV PYTHONPATH=/opt/airflow:/opt/airflow/scripts

