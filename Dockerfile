FROM python:3.10-slim

# Set env
ENV AIRFLOW_HOME=/opt/airflow
WORKDIR $AIRFLOW_HOME

# System dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && apt-get clean

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Setup directories
RUN mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/logs $AIRFLOW_HOME/plugins $AIRFLOW_HOME/output && \
    chmod -R 777 $AIRFLOW_HOME

ENV PATH="/root/.local/bin:$PATH"
