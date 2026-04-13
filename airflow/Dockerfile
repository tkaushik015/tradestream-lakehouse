FROM apache/airflow:2.9.3

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements-silver.txt /tmp/requirements-silver.txt

RUN pip install --no-cache-dir -r /tmp/requirements-silver.txt
