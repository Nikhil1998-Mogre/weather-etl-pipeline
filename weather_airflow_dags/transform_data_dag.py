"""
==================================================================================
TRANSFORM DATA DAG - Process Weather Data with Apache Spark & Load to BigQuery
==================================================================================
PURPOSE:
  This DAG processes raw weather forecast CSV data from GCS using Apache Spark,
  applies transformations (cleaning, type casting, renaming), and writes the
  processed data to BigQuery table for analytics.

WORKFLOW:
  1. Triggered by: extract_data_dag (openweather_api_to_gcs)
  2. Executes: Spark job (weather_data_processing.py) on Google Dataproc Serverless
  3. Input: Raw CSV from gs://weather-data-gds-my/weather/{date}/forecast.csv
  4. Output: Cleaned data loaded to BigQuery (project.forecast.weather_data)

TECHNOLOGY STACK:
  - Apache Spark 2.2 for distributed data processing
  - Google Dataproc Serverless (no cluster management needed)
  - PySpark for Python-based transformations
  - Google Cloud Storage for input data
  - BigQuery for data warehouse

SCHEDULE: Manual/triggered by upstream DAG
OWNER: Airflow
==================================================================================
"""

# ==================================================================================
# IMPORTS - Required modules and operators
# ==================================================================================
from datetime import datetime, timedelta  # For date manipulation
import uuid  # For generating unique batch IDs
from airflow import DAG  # Airflow DAG class
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator  # Operator to submit Spark jobs to Dataproc

# ==================================================================================
# DEFAULT ARGUMENTS - Configuration for all tasks in this DAG
# ==================================================================================
default_args = {
    'owner': 'airflow',  # Task owner (for notifications & audit logs)
    'depends_on_past': False,  # Don't block on previous DAG runs
    'retries': 1,  # Retry failed tasks once
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes before retry
    'start_date': datetime(2025, 5, 15),  # First possible DAG execution date
}

# ==================================================================================
# DAG DEFINITION
# ==================================================================================
with DAG(
    dag_id="transformed_weather_data_to_bq",  # Unique identifier for this DAG
    default_args=default_args,  # Apply default settings to all tasks
    schedule_interval=None,  # No automatic schedule; triggered by extract_data_dag
    catchup=False,  # Don't backfill past dates
) as dag:
    """
    DAG CONFIGURATION EXPLANATION:
    - dag_id: Unique name used to reference this DAG in Airflow UI
    - default_args: Shared settings (owner, retries, dates) for all tasks
    - schedule_interval=None: DAG runs only when triggered, not on a schedule
    - catchup=False: If DAG is paused, don't run missed scheduled intervals
    """

    # ==================================================================================
    # SECTION 1: GENERATE UNIQUE BATCH ID
    # ==================================================================================
    # Each Dataproc Batch job needs a unique ID for tracking & logging purposes
    # uuid.uuid4() generates a random unique identifier, we take first 8 chars for brevity
    batch_id = f"weather-data-batch-{str(uuid.uuid4())[:8]}"  # Example: "weather-data-batch-a1b2c3d4"

    # ==================================================================================
    # SECTION 2: CONFIGURE PYSPARK BATCH JOB
    # ==================================================================================
    # This dictionary defines all configuration for the Spark job execution
    # It specifies what code to run, runtime environment, and GCP infrastructure
    batch_details = {
        # PySpark batch configuration: Define the main Spark job
        "pyspark_batch": {
            # Path to the main Python script in GCS (contains the data transformation logic)
            # Script: weather_data_processing.py (reads CSV, transforms data, writes to BigQuery)
            "main_python_file_uri": f"gs://weather-data-gds-my/script/weather_data_processing.py",
            
            # Additional Python files to include (dependencies, utilities)
            # Empty in this case: all code is in main_python_file_uri
            "python_file_uris": [],
            
            # JAR files (Java libraries) to include on Spark classpath
            # Empty in this case: BigQuery connector is pre-installed in Dataproc
            "jar_file_uris": [],
            
            # Command-line arguments to pass to the Python script
            # Empty in this case: configurations are hardcoded in weather_data_processing.py
            "args": []
        },
        
        # Runtime configuration: Specify Spark version
        "runtime_config": {
            # Dataproc runtime version (includes Spark 2.2 and all required dependencies)
            "version": "2.2",
        },
        
        # Environment configuration: Specify GCP resources and networking
        "environment_config": {
            # Execution configuration: Define how and where the job runs
            "execution_config": {
                # Service account: GCP identity used to access GCS, BigQuery, etc.
                # Permissions must include:
                #   - Storage access (read/write to GCS buckets)
                #   - BigQuery admin (create tables, write data)
                #   - Service account user permissions
                "service_account": "550847718498-compute@developer.gserviceaccount.com",
                
                # VPC Network: Which Google Cloud network to use
                # "default" is the default VPC network in the project
                # Ensures job can communicate securely within your GCP environment
                "network_uri": "projects/project-ad501b9d-ed36-4c9f-91c/global/networks/default",
                
                # VPC Subnetwork: Specific subnet within the network
                # us-central1: Google Cloud region where resources are provisioned
                # Impacts latency, data residency, and compliance requirements
                "subnetwork_uri": "projects/project-ad501b9d-ed36-4c9f-91c/regions/us-central1/subnetworks/default",
            }
        },
    }

    # ==================================================================================
    # SECTION 3: CREATE DATAPROC BATCH OPERATOR
    # ==================================================================================
    # The DataprocCreateBatchOperator submits the Spark job to Google Dataproc Serverless
    # Advantages of Serverless:
    #   - No cluster management (Google manages infrastructure)
    #   - Pay only for job execution (no idle cluster costs)
    #   - Auto-scaling based on job requirements
    #   - Job runs in isolated Dataproc environment
    
    pyspark_task = DataprocCreateBatchOperator(
        # Task identification & naming
        task_id="spark_job_on_dataproc_create_serverless",  # Unique name within DAG
        
        # Batch job configuration (defined above)
        batch=batch_details,  # Contains PySpark code location, runtime config, network setup
        batch_id=batch_id,  # Unique ID for tracking this batch job execution
        
        # GCP Project & Location
        project_id="project-ad501b9d-ed36-4c9f-91c",  # GCP project where Dataproc batch runs
        region="us-central1",  # Google Cloud region (impacts latency & data locality)
        
        # Airflow connection configuration
        gcp_conn_id="google_cloud_default",  # Airflow connection name for GCP authentication
        # Note: Must be pre-configured in Airflow UI (Admin → Connections)
    )

    # ==================================================================================
    # SECTION 4: DEFINE TASK DEPENDENCIES
    # ==================================================================================
    # Specify which tasks must run and in what order
    # This DAG has only one task, so no complex dependencies needed
    pyspark_task
    # In a multi-task DAG, you would use: task1 >> task2 >> task3
    # (>> means "depends on" or "runs after")