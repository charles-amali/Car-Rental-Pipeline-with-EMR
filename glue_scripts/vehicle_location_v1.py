import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, countDistinct, to_timestamp
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Vehicle and Location Performance Metrics") \
        .getOrCreate()

    # Define schemas
    vehicles_schema = StructType([
        StructField("active", IntegerType(), True),
        StructField("vehicle_license_number", IntegerType(), True),
        StructField("registration_name", StringType(), True),
        StructField("license_type", StringType(), True),
        StructField("expiration_date", StringType(), True),
        StructField("permit_license_number", StringType(), True),
        StructField("certification_date", StringType(), True),
        StructField("vehicle_year", IntegerType(), True),
        StructField("base_telephone_number", StringType(), True),
        StructField("base_address", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("last_update_timestamp", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("vehicle_type", StringType(), True),
    ])

    # locations schema
    locations_schema = StructType([
        StructField("location_id", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
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

    vehicles_path = "s3://car-rentalplace/raw/vehicles.csv"
    locations_path = "s3://car-rentalplace/raw/locations.csv"
    rental_txn_path = "s3://car-rentalplace/raw/rental_transactions.csv"
    
    # Output paths - change these to your S3 locations
    location_performance_output = "s3://car-rentalplace/processed/location_performance/"
    vehicle_type_performance_output = "s3://car-rentalplace/processed/vehicle_type_performance/"
    top_locations_output = "s3://car-rentalplace/processed/top_locations/"
    top_vehicles_output = "s3://car-rentalplace/processed/top_vehicles/"

    # Load the data
    try:
        vehicles_df = spark.read.csv(vehicles_path, header=True, schema=vehicles_schema)
        locations_df = spark.read.csv(locations_path, header=True, schema=locations_schema)
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

        # Convert location IDs to string for joining
        rental_txn_df = rental_txn_df.withColumn(
            "pickup_location", 
            col("pickup_location").cast("string")
        ).withColumn(
            "dropoff_location", 
            col("dropoff_location").cast("string")
        )

        # Join with vehicles to get vehicle type information
        rental_vehicle_df = rental_txn_df.join(vehicles_df, on="vehicle_id", how="left")
        
        logger.info("Data transformation completed")

        # 1. Location Performance Metrics
        location_performance = rental_txn_df.groupBy("pickup_location") \
            .agg(
                sum("total_amount").alias("total_revenue"), 
                count("rental_id").alias("total_transactions"),
                avg("total_amount").alias("avg_transaction_amount"),
                max("total_amount").alias("max_transaction_amount"),
                min("total_amount").alias("min_transaction_amount"),
                countDistinct("vehicle_id").alias("unique_vehicles")
            )

        # Join with location names
        location_performance = location_performance.join(
            locations_df, 
            location_performance.pickup_location == locations_df.location_id, 
            "left"
        ).select(
            col("pickup_location"), 
            col("location_name"),
            col("city"),
            col("state"),
            col("total_revenue"), 
            col("total_transactions"), 
            col("avg_transaction_amount"),
            col("max_transaction_amount"),
            col("min_transaction_amount"),
            col("unique_vehicles")
        )
        
        # 2. Vehicle Type Performance Metrics
        vehicle_type_performance = rental_vehicle_df.groupBy("vehicle_type") \
            .agg(
                avg("rental_duration_hours").alias("avg_rental_duration"),
                max("rental_duration_hours").alias("max_rental_duration"),
                min("rental_duration_hours").alias("min_rental_duration"),
                sum("total_amount").alias("total_revenue"),
                count("rental_id").alias("total_rentals"),
                avg("total_amount").alias("avg_revenue_per_rental")
            )
        
        # 3. Top revenue-generating locations
        top_locations = location_performance.orderBy(col("total_revenue").desc())

        # 4. Most rented vehicle types
        top_vehicles = rental_vehicle_df.groupBy("vehicle_type", "brand") \
            .agg(
                count("rental_id").alias("total_rentals"),
                sum("total_amount").alias("total_revenue")
            ) \
            .orderBy(col("total_rentals").desc())
        
        logger.info("KPI calculations completed")

        # Write results to Parquet format
        location_performance.write.mode("overwrite").parquet(location_performance_output)
        vehicle_type_performance.write.mode("overwrite").parquet(vehicle_type_performance_output)
        top_locations.write.mode("overwrite").parquet(top_locations_output)
        top_vehicles.write.mode("overwrite").parquet(top_vehicles_output)
        
        logger.info("Results written to S3")
        
        # Display sample results (in EMR this would be logged)
        logger.info("Sample Location Performance:")
        location_performance.show(5, False)
        
        logger.info("Sample Vehicle Type Performance:")
        vehicle_type_performance.show(5, False)
        
        logger.info("Top Revenue Locations:")
        top_locations.show(5, False)
        
        logger.info("Top Rented Vehicles:")
        top_vehicles.show(5, False)
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()