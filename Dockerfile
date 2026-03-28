FROM apache/airflow:2.10.1

USER root
RUN apt-get update && \
    apt-get install default-jre -y

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64/
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt /requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /requirements.txt

# Install PySpark explicitly
RUN pip install findspark