#!/usr/bin/env python3
"""
==================================================================================
WEATHER DATA PROCESSING SPARK JOB
==================================================================================
PURPOSE:
  This PySpark script processes raw weather forecast CSV data from Google Cloud Storage,
  applies data cleaning and transformations, and loads the processed data into BigQuery.

WORKFLOW:
  1. Read raw CSV from GCS (gs://weather-data-gds-my/weather/{date}/forecast.csv)
  2. Parse timestamps and extract weather conditions
  3. Cast data types (integers, doubles, longs) for BigQuery compatibility
  4. Rename columns for clarity and consistency
  5. Write processed DataFrame to BigQuery table (forecast.weather_data)

INPUT:
  - CSV file containing OpenWeather API forecast data (flattened JSON structure)
  
OUTPUT:
  - BigQuery table with cleaned, typed, and renamed columns ready for analysis

EXECUTION:
  - Runs on Google Cloud Dataproc Serverless (no cluster management)
  - Triggered by Airflow DAG (transform_data_dag.py)
  - Uses BigQuery connector for Spark for direct BQ integration

KEY TRANSFORMATIONS:
  - Unix timestamp → Datetime
  - String weather JSON → Weather condition text (via regex)
  - Nested column names → Flat, renamed columns
  - String values → Typed columns (int, double, long)
==================================================================================
"""

# ==================================================================================
# IMPORTS - Required PySpark and Python libraries
# ==================================================================================
#!/usr/bin/env python3  # Shebang: allows script to run as executable on Unix/Linux systems

import os, datetime  # os: file operations; datetime: work with dates/times
from pyspark.sql import SparkSession  # Main Spark API for distributed data processing
from pyspark.sql.functions import col, from_unixtime, to_timestamp, regexp_extract
# Functions explanation:
#   - col: Select/reference a column from DataFrame
#   - from_unixtime: Convert Unix timestamp (seconds since 1970) to datetime string
#   - to_timestamp: Parse string to timestamp type
#   - regexp_extract: Extract text using regex pattern matching

def main():
    # —────────────── CONFIG ─────────────────────────────────────────
    # Configuration variables for BigQuery target and GCS paths
    # These define where data comes from and where it goes
    
    project      = "project-ad501b9d-ed36-4c9f-91c"  # GCP project ID (where BigQuery table lives)
    dataset      = "forecast"  # BigQuery dataset name (container for tables)
    table        = "weather_data"  # BigQuery table name (final destination)
    temp_bucket  = "bq-temp-gds-nikhil"  # GCS bucket for temporary Spark-BigQuery staging files
                                          # BigQuery connector uses this to transfer large datasets efficiently
    bucket       = "weather-data-gds-my"  # GCS bucket containing raw weather CSV data
    today        = datetime.date.today().strftime("%Y-%m-%d")  # Current date in YYYY-MM-DD format
                                                                # Used to find today's data file
    input_path   = f"gs://{bucket}/weather/{today}/forecast.csv"  # Full GCS path to input CSV
                                                                     # Example: gs://weather-data-gds-my/weather/2026-05-03/forecast.csv

    # —──────────── SPARK SESSION ───────────────────────────────────
    # Initialize SparkSession: The entry point for Spark functionality
    # On Dataproc Serverless, Google manages resource allocation automatically
    # .getOrCreate() reuses existing session if one already exists
    
    spark = (
        SparkSession.builder
          .appName("WeatherDataProcessing")  # Name for this Spark application (shows in cluster UI)
          .getOrCreate()  # Get existing session or create new one
    )

    # —────────────── READ & INFER ──────────────────────────────────
    # Read CSV from GCS and infer schema (column names and types)
    # "Infer" means Spark scans the data to guess data types (string, integer, double, etc.)
    # This is useful when you don't have a predefined schema
    
    df = (
        spark.read
             .option("header", True)  # First row contains column names
             .option("quote", '"')  # Column values are quoted with double quotes
             .option("sep", ",")  # Columns separated by commas
             .option("inferSchema", True)  # Spark scans data to determine column types
             .csv(input_path)  # Read CSV file from GCS path (gs://...)
    )
    # Result: DataFrame 'df' with columns inferred from CSV headers and data types guessed from values

    # —────────── CAST & RENAME ─────────────────────────────────────
    # Transform the raw DataFrame:
    # 1. Convert timestamps from Unix format to proper datetime
    # 2. Extract weather condition from stringified JSON array using regex
    # 3. Cast all columns to correct types (int, double, long)
    # 4. Rename columns for clarity and consistency
    # "Casting" = converting data from one type to another (e.g., string → integer)
    # "Aliases" = renaming columns to shorter/cleaner names
    
    df2 = (
        df
        # TIMESTAMP CONVERSIONS
        # Convert Unix timestamp (seconds since 1970-01-01) to datetime string, then to timestamp type
        .withColumn("dt", from_unixtime(col("dt")).cast("timestamp"))
        
        # Parse string timestamp to Spark timestamp type using specific format
        .withColumn("dt_txt", to_timestamp(col("dt_txt"), "yyyy-MM-dd HH:mm:ss"))
        
        # WEATHER EXTRACTION
        # Extract weather condition from stringified list using regex pattern matching
        # Input:  "[{'id': 500, 'main': 'Rain', 'description': 'light rain', 'icon': '10d'}]"
        # Pattern: r"'main':\s*'([^']+)'"  → captures text between quotes after 'main':
        # Output: "Rain"
        # Group 1 (the first captured group) contains the extracted value
        .withColumn("weather_main", regexp_extract(col("weather"), r"'main':\s*'([^']+)'", 1))
        
        # SELECT AND RENAME: Choose columns and rename them for BigQuery
        .select(
            # Renamed columns (dt timestamp, forecast_time as alias)
            col("dt").alias("dt"),
            col("dt_txt").alias("forecast_time"),  # More descriptive name
            col("weather_main").alias("weather"),  # Use extracted weather value
            
            # Visibility column: cast string to integer (convert km to standard format)
            col("visibility").cast("int").alias("visibility"),
            
            # Probability of precipitation: cast to decimal
            col("pop").cast("double").alias("pop"),
            
            # Temperature fields: extract from nested columns, cast to double (decimal)
            # Nested columns use backticks because they contain dots
            col("`main.temp`").cast("double").alias("temp"),  # Current temperature
            col("`main.feels_like`").cast("double").alias("feels_like"),  # "Feels like" temperature
            col("`main.temp_min`").cast("double").alias("min_temp"),  # Minimum temperature
            col("`main.temp_max`").cast("double").alias("max_temp"),  # Maximum temperature
            
            # Pressure fields: cast to long (64-bit integer for large numbers)
            col("`main.pressure`").cast("long").alias("pressure"),  # Atmospheric pressure (hPa)
            col("`main.sea_level`").cast("long").alias("sea_level"),  # Pressure at sea level
            col("`main.grnd_level`").cast("long").alias("ground_level"),  # Pressure at ground level
            
            # Humidity: integer percentage (0-100)
            col("`main.humidity`").cast("long").alias("humidity"),
            
            # Temperature difference from other forecasts: decimal
            col("`main.temp_kf`").cast("double").alias("temp_kf"),
            
            # Cloud coverage: percentage (0-100) as integer
            col("`clouds.all`").cast("long").alias("clouds_all"),
            
            # Wind properties: speed in m/s, direction in degrees, gust as decimal
            col("`wind.speed`").cast("double").alias("wind_speed"),
            col("`wind.deg`").cast("long").alias("wind_deg"),  # Wind direction (0-360 degrees)
            col("`wind.gust`").cast("double").alias("wind_gust"),  # Maximum wind gust speed
            
            # System data: day/night indicator (d=day, n=night)
            col("`sys.pod`").alias("sys_pod"),
            
            # Rainfall: precipitation in mm over 3 hours (only if it rained)
            col("`rain.3h`").cast("double").alias("rain_3h"),
        )
    )

    # —────────── WRITE TO BIGQUERY ─────────────────────────────────
    # Write the transformed DataFrame to BigQuery table
    # Uses BigQuery connector for Spark (spark-bigquery) which handles:
    # - Type conversions (Spark types → BigQuery types)
    # - Automatic table creation if it doesn't exist
    # - Secure authentication via GCP service account
    # - Efficient bulk loading via temporary GCS staging files
    
    (
        df2.write  # Begin write operation on transformed DataFrame
           .format("bigquery")  # Use BigQuery as target format
           .option("table", f"{project}.{dataset}.{table}")  # Full table path: project.dataset.table
           .option("temporaryGcsBucket", temp_bucket)  # GCS bucket for staging files during transfer
                                                         # Required for large datasets (BQ connector best practice)
           .option("createDisposition", "CREATE_IF_NEEDED")  # Automatically create table if it doesn't exist
                                                               # Other options: MUST_EXIST (fail if table missing)
           .option("writeDisposition", "WRITE_APPEND")  # Append data to existing table
                                                        # Other options: WRITE_TRUNCATE (overwrite), 
                                                        # WRITE_EMPTY (fail if table has data)
           .save()  # Execute the write operation
    )

    # Stop Spark session to free up resources
    # Important: Ensures cluster is properly cleaned up, prevents resource leaks
    spark.stop()

# ==================================================================================
# SCRIPT EXECUTION ENTRY POINT
# ==================================================================================
if __name__ == "__main__":
    # This condition ensures main() only runs when script is executed directly
    # (Not when imported as a module in another script)
    # Allows Airflow/other systems to safely import this file without auto-running main()
    main()