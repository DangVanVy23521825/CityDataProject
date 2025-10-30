# Smart City Real-Time Data Streaming Pipeline  
End-to-End IoT → Kafka → Spark → S3 → Glue → Athena → Dashboard

## Overview
This project implements a **real-time Smart City data streaming system** that simulates IoT vehicle movement from **Ho Chi Minh City → Da Nang**, collecting:

- Vehicle telemetry  
- GPS telemetry  
- Traffic congestion data (TomTom API)  
- Weather & air-quality data (OpenWeather API)  

The pipeline streams data in real-time using **Kafka**, processes it with **Apache Spark Structured Streaming**, orchestrates with **Airflow**, and stores cleaned data in **AWS S3**, where **AWS Glue** catalogs it for querying via **Athena** and visualization dashboards.

> **Goal:** Demonstrate a modern real-time data engineering stack for IoT + smart transportation systems.

---

## 🚀 Architecture

## Technologies Used

- **IoT Devices**: For capturing real-time data.
- **Apache Zookeeper**: For managing and coordinating Kafka brokers.
- **Apache Kafka**: For real-time data ingestion into different topics.
- **Apache Spark**: For real-time data processing and streaming.
- **Docker**: For containerization and orchestration of Kafka and Spark.
- **Python**: For data processing scripts.
- **AWS Cloud**: 
  - **S3**: For storing processed data as Parquet files.
  - **Glue**: For data extraction and cataloging.
  - **Athena**: For querying processed data.
  - **IAM**: For managing access and permissions.
- **Amazon QuickSight**: For data visualization and dashboarding.

## Project Workflow

1. **Data Ingestion**:
   - IoT devices capture real-time data.
   - Data is ingested into Kafka topics configured in Docker using `docker-compose.yml`.

2. **Data Processing**:
   - Apache Spark reads data from Kafka topics.
   - Spark processes the data and writes it to AWS S3 as Parquet files.
   - Spark Streaming is used for real-time data processing with checkpointing to handle data issues.

3. **Data Storage**:
   - Processed data is stored in AWS S3.
   - AWS Glue crawlers extract data from S3 and catalog it.

4. **Data Querying**:
   - AWS Athena queries the processed and cataloged data from Glue.

5. **Data Visualization**:
   - Amazon QuickSight visualizes the queried data with interactive dashboards.

This project demonstrates the power of modern data engineering tools to handle complex, real-time data streams and deliver actionable insights for Smart City initiatives. The use of AWS services ensures scalability, reliability, and ease of data management, making it an excellent example of an end-to-end data streaming pipeline.
Thanks to the open-source community for providing the tools and libraries used in this project.
