#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, date_format, to_timestamp, when, round
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Create Spark session
    spark = SparkSession.builder \
        .appName("User and Transaction Analysis") \
        .getOrCreate()

    # Define schemas
    users_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("driver_license_number", StringType(), True),
        StructField("driver_license_expiry", DateType(), True),
        StructField("creation_date", DateType(), True),
        StructField("is_active", IntegerType(), True),
    ])

    # rental locations schema
    rental_transactions_schema = StructType([
        StructField("rental_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("rental_start_time", StringType(), True),
        StructField("rental_end_time", StringType(), True),
        StructField("pickup_location", IntegerType(), True),
        StructField("dropoff_location", IntegerType(), True),
        StructField("total_amount", DoubleType(), True)
    ])

    # Input paths 
    users_path = "s3://car-rentalplace/raw/users.csv"
    rental_txn_path = "s3://car-rentalplace/raw/rental_transactions.csv"
    
    # Output paths 
    daily_metrics_output = "s3://car-rentalplace/processed/daily_metrics/"
    user_metrics_output = "s3://car-rentalplace/processed/user_metrics/"
    top_users_output = "s3://car-rentalplace/processed/top_users/"
    avg_transaction_output = "s3://car-rentalplace/processed/avg_transaction_value/"

    try:
        # Load the data
        users_df = spark.read.csv(users_path, header=True, schema=users_schema)
        rental_txn_df = spark.read.csv(rental_txn_path, header=True, schema=rental_transactions_schema)
        
        logger.info("Data loaded successfully")
        
        # Process rental transactions
        rental_txn_df = rental_txn_df.withColumn(
            "rental_start_time", 
            to_timestamp(col("rental_start_time"), "yyyy-MM-dd HH:mm:ss")
        ).withColumn(
            "rental_end_time", 
            to_timestamp(col("rental_end_time"), "yyyy-MM-dd HH:mm:ss")
        )

        # Calculate rental duration in hours
        rental_txn_df = rental_txn_df.withColumn(
            "rental_duration_hours",
            (col("rental_end_time").cast("long") - col("rental_start_time").cast("long")) / 3600
        )

        # Extract date from rental_start_time for daily metrics
        rental_txn_df = rental_txn_df.withColumn(
            "rental_date", 
            date_format(col("rental_start_time"), "yyyy-MM-dd")
        )
        
        logger.info("Data transformation completed")

        # 1. Daily Transaction Metrics
        daily_metrics = rental_txn_df.groupBy("rental_date") \
            .agg(
                count("rental_id").alias("total_transactions"),
                sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_transaction_amount"),
                max("total_amount").alias("max_transaction_amount"),
                min("total_amount").alias("min_transaction_amount"),
                sum("rental_duration_hours").alias("total_rental_hours")
            ) \
            .orderBy("rental_date")

        # 2. Overall average transaction value
        avg_transaction_value = rental_txn_df.agg(
            avg("total_amount").alias("avg_transaction_value"),
            min("total_amount").alias("min_transaction_value"),
            max("total_amount").alias("max_transaction_value"),
            count("rental_id").alias("total_transactions"),
            sum("total_amount").alias("total_revenue")
        )

        # 3. User engagement metrics
        user_metrics = rental_txn_df.groupBy("user_id") \
            .agg(
                count("rental_id").alias("total_transactions"),
                sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_spending"),
                max("total_amount").alias("max_spending"),
                min("total_amount").alias("min_spending"),
                sum("rental_duration_hours").alias("total_rental_hours"),
                avg("rental_duration_hours").alias("avg_rental_hours")
            )

        # Join with user information
        user_metrics = user_metrics.join(users_df, on="user_id", how="left") \
            .select(
                "user_id", "first_name", "last_name", "email", "is_active",
                "total_transactions", "total_revenue", "avg_spending", 
                "max_spending", "min_spending", "total_rental_hours", "avg_rental_hours"
            )

        # 4. Top spending users
        top_users = user_metrics.orderBy(col("total_revenue").desc())
        
        logger.info("KPI calculations completed")

        # Write results to Parquet format
        daily_metrics.write.mode("overwrite").parquet(daily_metrics_output)
        user_metrics.write.mode("overwrite").parquet(user_metrics_output)
        top_users.write.mode("overwrite").parquet(top_users_output)
        avg_transaction_value.write.mode("overwrite").parquet(avg_transaction_output)
        
        logger.info("Results written to S3")
        
        # Log sample results
        logger.info("Sample Daily Metrics:")
        daily_metrics.show(5, False)
        
        logger.info("Overall Transaction Metrics:")
        avg_transaction_value.show(False)
        
        logger.info("Sample User Metrics:")
        user_metrics.show(5, False)
        
        logger.info("Top Spending Users:")
        top_users.show(5, False)
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()