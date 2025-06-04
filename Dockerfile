FROM apache/airflow:2.10.5-python3.12

ARG SPARK_VERSION="3.5.2"
ARG HADOOP_VERSION="3"

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk wget unzip zip ant && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -O /tmp/spark.tgz && \
    mkdir -p $SPARK_HOME && \
    tar -xzf /tmp/spark.tgz -C $SPARK_HOME --strip-components=1 && \
    rm /tmp/spark.tgz

# COPY ./airflow_start.sh /usr/bin/airflow_start.sh
# RUN chmod +x /usr/bin/airflow_start.sh

USER airflow
RUN pip install --no-cache-dir "apache-airflow-providers-apache-spark==4.3.0"