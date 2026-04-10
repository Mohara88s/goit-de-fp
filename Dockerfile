FROM apache/airflow:3.1.8

USER root
# Встановлюємо Java (необхідна для PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# Встановлюємо PySpark
RUN pip install --no-cache-dir pyspark