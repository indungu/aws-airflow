FROM puckel/docker-airflow:1.10.9
USER root
ENV AIRFLOW_USER_HOME=/usr/local/airflow
COPY airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
USER airflow
RUN mkdir ${AIRFLOW_USER_HOME}/dags
COPY *dag*.py ${AIRFLOW_USER_HOME}/dags

