FROM apache/airflow:latest

COPY ./dags/requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

USER root

RUN apt-get update && apt-get install -y \
    wget

COPY ./scripts/airflow_entrypoint.sh /airflow_entrypoint.sh
RUN chmod +x /airflow_entrypoint.sh
USER airflow
ENTRYPOINT ["/bin/bash","/airflow_entrypoint.sh"]