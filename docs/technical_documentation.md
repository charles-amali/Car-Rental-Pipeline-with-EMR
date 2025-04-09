# Car Rental Marketplace Analytics - Technical Documentation

## Table of Contents
1. [System Architecture](#1-system-architecture)
2. [Data Pipeline](#2-data-pipeline)
3. [Infrastructure Components](#3-infrastructure-components)
4. [Data Processing](#4-data-processing)
5. [Monitoring and Logging](#5-monitoring-and-logging)
6. [Security and Compliance](#6-security-and-compliance)
7. [Troubleshooting Guide](#7-troubleshooting-guide)
8. [Deployment Guide](#8-deployment-guide)

## 1. System Architecture

### 1.1 High-Level Architecture
The system implements a serverless data processing pipeline using AWS services:
- Data Storage: Amazon S3
- Processing: AWS EMR (Elastic MapReduce)
- Orchestration: AWS Step Functions
- Metadata Management: AWS Glue
- Query Engine: Amazon Athena
- Monitoring: CloudWatch, SNS

### 1.2 Data Flow
1. Raw data ingestion to S3
2. EMR cluster processes data using Spark
3. Processed data written back to S3
4. Glue Crawler updates Data Catalog
5. Athena queries generate insights

## 2. Data Pipeline

### 2.1 Data Sources
```plaintext
s3://car-rentalplace/raw/
├── locations.csv
│   ├── location_id (INT)
│   ├── location_name (STRING)
│   └── geographic_coordinates (STRING)
├── vehicles.csv
│   ├── vehicle_id (INT)
│   ├── vehicle_type (STRING)
│   └── status (STRING)
├── users.csv
│   ├── user_id (INT)
│   ├── registration_date (TIMESTAMP)
│   └── user_details (STRING)
└── rental_transactions.csv
    ├── transaction_id (INT)
    ├── user_id (INT)
    ├── vehicle_id (INT)
    ├── rental_start_time (TIMESTAMP)
    ├── rental_end_time (TIMESTAMP)
    └── total_amount (DECIMAL)
```

### 2.2 Processing Jobs
#### 2.2.1 Vehicle Location Metrics
```python
# Key transformations
def compute_location_kpis(transactions_df, locations_df):
    return transactions_df.groupBy("pickup_location") \
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("*").alias("total_transactions"),
            F.avg("total_amount").alias("avg_transaction_amount")
        )
```

#### 2.2.2 User Transaction Analysis
```python
# Core metrics computation
def compute_user_metrics(transactions_df):
    return transactions_df.groupBy("user_id") \
        .agg(
            F.sum("total_amount").alias("total_spent"),
            F.count("*").alias("total_rentals"),
            F.avg("rental_duration").alias("avg_rental_duration")
        )
```

## 3. Infrastructure Components

### 3.1 EMR Cluster Configuration
```json
{
    "Name": "Spark-EMR",
    "ReleaseLabel": "emr-7.7.0",
    "Applications": ["Spark", "Hadoop", "Hive"],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1
            },
            {
                "Name": "Core",
                "InstanceRole": "CORE",
                "InstanceType": "r5.2xlarge",
                "InstanceCount": 2
            }
        ]
    }
}
```

### 3.2 Data Processing Jobs
-Vehicle Location Analysis
     ```python
     def compute_location_kpis(transactions_df):
         return transactions_df.groupBy("pickup_location") \
             .agg(
                 F.sum("total_amount").alias("total_revenue"),
                 F.count("*").alias("total_transactions"),
                 F.avg("total_amount").alias("avg_transaction_amount")
             )
     ```
- User Transaction Analysis
     ```python
     def compute_user_metrics(transactions_df):
         return transactions_df.groupBy("user_id") \
             .agg(
                 F.sum("total_amount").alias("total_spent"),
                 F.count("*").alias("total_rentals"),
                 F.avg("rental_duration").alias("avg_rental_duration")
             )
     ```

#### 3.2.1 Data Schema Specifications

1. **Input Schemas**
   ```python
   users_schema = StructType([
       StructField("user_id", StringType(), True),
       StructField("first_name", StringType(), True),
       StructField("last_name", StringType(), True),
       StructField("email", StringType(), True),
       StructField("driver_license_number", StringType(), True),
       StructField("driver_license_expiry", DateType(), True),
       StructField("creation_date", DateType(), True),
       StructField("is_active", IntegerType(), True)
   ])
   ```

2. **Output Schemas**
   ```sql
   CREATE TABLE location_performance (
       location_id INT,
       total_revenue DECIMAL(18,2),
       transaction_count INT,
       avg_transaction_amount DECIMAL(10,2),
       utilization_rate FLOAT
   );

   CREATE TABLE user_metrics (
       user_id INT,
       total_spent DECIMAL(18,2),
       rental_count INT,
       avg_rental_duration FLOAT
   );
   ```

## 4. Workflow Orchestration

1. **Step Functions State Machine**
   - Primary Workflow:
     ```json
     {
         "StartAt": "Start EMR Cluster",
         "States": {
             "Start EMR Cluster": {
                 "Type": "Task",
                 "Resource": "arn:aws:states:::elasticmapreduce:createCluster.sync",
                 "Next": "Parallel Processing"
             }
         }
     }
     ```

2. **Error Handling and Retries**
   - EMR Job Failures: Automatic retry with exponential backoff
   - Data Quality Issues: Validation checks before processing
   - System Errors: SNS notifications for manual intervention

## 5. Performance Optimization

1. **Spark Configurations**
   ```python
   spark.conf.set("spark.sql.shuffle.partitions", "200")
   spark.conf.set("spark.memory.fraction", "0.8")
   spark.conf.set("spark.executor.memory", "8g")
   ```

2. **Data Partitioning Strategy**
   - Location data: Partitioned by geographic region
   - Transaction data: Partitioned by date
   - User data: Partitioned by registration year

## 6. Monitoring and Logging

1. **CloudWatch Metrics**
   - EMR Cluster Health
   - Job Duration
   - Data Processing Volume
   - Error Rates

2. **Alert Thresholds**
   ```json
   {
       "JobFailureAlarm": {
           "MetricName": "JobsFailed",
           "Threshold": 1,
           "Period": 300,
           "EvaluationPeriods": 1
       }
   }
   ```

## 7. Security Implementation

1. **Data Encryption**
   - S3: Server-side encryption (SSE-S3)
   - EMR: Encryption at rest
   - Network: TLS 1.2 for data in transit

2. **Access Control**
   - IAM Roles for EMR and Step Functions
   - S3 Bucket Policies
   - VPC Security Groups

## .8 Troubleshooting Guide

### 8.1 Common Issues
1. EMR Cluster Failure
   - Check EMR logs in `s3://car-rentalplace/emr-logs/`
   - Verify IAM roles and permissions

2. Data Quality Issues
   - Check validation logs
   - Verify source data integrity

### 8.2 Recovery Procedures
1. Job Failure Recovery
   - Automated retry mechanism
   - Manual intervention steps

## 9. Deployment Guide

### 9.1 Prerequisites
- AWS CLI configured
- Required IAM roles created
- S3 buckets provisioned

### 9.2 Deployment Steps

 **Data Pipeline Deployment**
   ```bash
   aws stepfunctions create-state-machine \
       --name car-rental-pipeline \
       --definition file://step_function_orchestration.json
   ```

### 9.3 Verification Steps
1. Validate S3 bucket structure
2. Verify EMR cluster creation
3. Test Step Functions workflow
4. Confirm Athena query access


