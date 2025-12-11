# Dengue Outbreak Prediction Data Pipeline

> Automated AWS Data Pipeline for Dengue Outbreak Prediction - A fully containerized, end-to-end data processing workflow built using modern AWS cloud services.

![Architecture Diagram](https://github.com/AlFrancis-Dagaang/dengue-outbreak-prediction-pipeline/blob/main/architecture/architecture-diagram.png)

##Overview

An automated, serverless data pipeline for dengue outbreak prediction that orchestrates three containerized jobs—data cleaning, feature engineering, and exploratory data analysis—using AWS Batch and Step Functions. The pipeline processes epidemiological data through isolated Docker containers, storing results across multiple S3 buckets with automated notifications and SQL query capabilities.


## Key Features

- **Fully Serverless** - Runs on AWS Fargate with automatic scaling
- **Containerized Jobs** - Isolated Docker containers for each processing stage
- **Automated Orchestration** - Step Functions manages the entire workflow
- **Real-time Notifications** - SNS alerts on pipeline status
- **Cost-Effective** - Pay only for compute time used
- **SQL Queryable** - Amazon Athena enables SQL analysis on processed data

## 🛠️ Tech Stack

- **AWS Batch (Fargate)** - Serverless container execution
- **AWS Step Functions** - Workflow orchestration
- **Amazon ECR** - Container registry
- **Amazon S3** - Data lake storage
- **Amazon SNS** - Notifications
- **Amazon Athena** - SQL queries
- **Docker** - Container packaging
- **Python** - Data processing

## 🔄 Pipeline Stages

### Stage 1: Data Cleaning
**Input**: Raw Data Bucket → **Output**: Cleaned Data Bucket  
Handles missing values, removes duplicates, validates data types, and eliminates outliers.

### Stage 2: Feature Engineering
**Input**: Cleaned Data Bucket → **Output**: Feature-Engineered Bucket  
Generates temporal features, creates lagged variables, encodes categories, and scales features.

### Stage 3: Exploratory Data Analysis
**Input**: Feature-Engineered Bucket → **Output**: EDA Output Bucket  
Produces statistical summaries, correlation matrices, and visualization plots.

