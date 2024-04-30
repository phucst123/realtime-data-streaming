# realtime-data-streaming



## Table of Contents

1. [Introduction](#introduction)
2. [Getting started](#getting-started)

## Introduction

This project is create a data processing system. It covers everything from getting the data to storing it, using tools like Apache Airflow, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra. Everything is packaged with Docker, making it easy to install and scale up as needed

## Getting started

Provide instructions on how to install the project, including any dependencies that need to be installed and how to install them. You can include code snippets or commands for package managers like pip or npm.

1. Clone the repo
```bash
git clone https://github.com/phucst123/realtime-data-streaming.git

```

2. Run Docker Compose
```bash
docker-compose up -d --build

```

3. Submit spark job
```bash
docker exec -it realtime-data-streaming-spark-master-1 spark-submit --master spark://<spark_master_ip>:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 jobs/spark_stream.py

```