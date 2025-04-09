# Car Rental Marketplace Analytics Pipeline

## Overview
This project implements an automated analytics pipeline for a car rental marketplace using AWS services including EMR, Step Functions, Glue, and Athena. The pipeline processes rental transaction data, computes various KPIs, and generates insights about vehicle utilization, location performance, and user behavior.

## Architecture
![Architecture Diagram](images/architectural_diagram(emr).jpg)
- **Data Processing**: AWS EMR cluster running Spark jobs
- **Workflow Orchestration**: AWS Step Functions
- **Data Catalog**: AWS Glue
- **Analytics**: Amazon Athena
- **Storage**: Amazon S3
- **Notifications**: Amazon SNS


## Key Components

### Data Sources
Raw data stored in S3 under `s3://your-bucket/raw/`:
- `locations.csv`: Rental location details
- `vehicles.csv`: Vehicle inventory information
- `users.csv`: User profiles
- `rental_transactions.csv`: Transaction records

### Spark Jobs
- `vehicle_location_v1.py`: Analyzes vehicle utilization and location-based performance metrics
- `user_transaction_kpis.py`: Processes user behavior and transaction-related KPIs

### Step Functions Workflows
Multiple workflow options available:
- `step_function_orchestration.json`: Analytics-focused workflow with dynamic query processing

[!Step Function Diagram](images/car-rental-flow.png)



[!Step Function Diagram](images/car-rental-step-flow.png)

*After a successful completion of the EMR cluster processing, the workflow triggers glue crawler to update the data catalog and then executes Athena queries to generate insights.*

## Setup and Dependencies

1. Install required Python packages:
```bash
pip install -r requirements.txt
```

2. Configure AWS credentials and environment variables:
```bash
export S3_BUCKET_NAME=your-bucket
```

3. Upload data to S3:
```bash
python upload_to_s3.py
```

## Key Metrics Generated

### Location Analytics
- Total revenue per location
- Transaction volume by location
- Geographic performance analysis

### Vehicle Analytics
- Vehicle type performance
- Utilization rates
- Revenue by vehicle category

### User Analytics
- User spending patterns
- Rental duration metrics
- Top customer analysis

## Output Data
Processed data is stored in S3 under various prefixes:
- `s3://your-bucket/processed/location_performance/`
- `s3://your-bucket/processed/vehicle_type_performance/`
- `s3://your-bucket/processed/user_metrics/`
- `s3://your-bucket/processed/daily_metrics/`

## Monitoring and Logging
- EMR logs available at `s3://your-bucket/emr-logs/`
- Query results stored at `s3://your-bucket/query-results/`
- SNS notifications for pipeline completion

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License
This project is licensed under the MIT License - see the LICENSE file for details.
