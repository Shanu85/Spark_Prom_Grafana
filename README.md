# Spark_Prom_Grafana

This project implements an end-to-end data pipeline with the following components:

### Data Ingestion:
A Java producer generates ~3B records/hour and publishes them to Kafka.

### Stream Processing:
We consume the Kafka data using PySpark Structured Streaming, perform aggregations, and push the processed data back into Kafka.

### Metrics & Monitoring:

Kafka (Broker/Controller) produces logs, which are collected using Log4J.

Application-level metrics are exported via the JMX Exporter, which converts Kafka’s JMX metrics into Prometheus-friendly formats.

Metrics are scraped by Prometheus and visualized in Grafana dashboards.

Alerting is handled through Alertmanager, integrated with Prometheus.

### Logging:

Application and Kafka logs are collected using Filebeat.

Logs are shipped to Elasticsearch and visualized in Kibana.

## DFD
<img width="2323" height="1564" alt="Untitled-2025-06-26-1114 excalidraw" src="https://github.com/user-attachments/assets/b65fb65b-2cc8-476b-bf1c-9005678b9181" />


## Jar files for kafka Producer
Java Version - openjdk 17.0.16
  1. jackson-annotations-2.19.0
  2. jackson-core-2.19.0
  3. jackson-databind-2.19.0
  4. kafka-clients-3.8.1
  5. lombok-1.18.20
  6. lz4-java-1.8.0
  7. slf4j-api-2.0.17
  8. slf4j-simple-2.0.17
