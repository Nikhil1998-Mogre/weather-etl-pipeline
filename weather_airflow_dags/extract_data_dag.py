"""
==================================================================================
EXTRACT DATA DAG - OpenWeather API to Google Cloud Storage
==================================================================================
PURPOSE:
  This DAG fetches real-time weather forecast data from OpenWeather API,
  converts it to CSV, uploads it to Google Cloud Storage (GCS),
  and triggers the downstream transformation DAG.

WORKFLOW:
  1. Extract: Call OpenWeather API for Toronto weather forecast
  2. Transform: Convert JSON response to pandas DataFrame → CSV string
  3. Upload: Save CSV to GCS bucket (gs://weather-data-gds-my/weather/{date}/)
  4. Trigger: Start the transform DAG to process and load data into BigQuery

SCHEDULE: Manual trigger (no automated schedule)
OWNER: Airflow
==================================================================================
"""

# Import required modules for Airflow DAG orchestration
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # Trigger other DAGs
from airflow.operators.python import PythonVirtualenvOperator  # Run Python in isolated environment
from airflow.operators.python import PythonOperator  # Run Python functions
from airflow.providers.google.cloud.hooks.gcs import GCSHook  # Connect to Google Cloud Storage
from airflow.models import Variable  # Access Airflow variables (like API keys)
from datetime import timedelta

# ==================================================================================
# DEFAULT ARGUMENTS - Configuration for all tasks in this DAG
# ==================================================================================
default_args = {
    "owner": "airflow",  # Task owner (for notification & audit purposes)
    "depends_on_past": False,  # Don't wait for previous DAG runs to succeed
    "retries": 1,  # Retry failed tasks 1 time
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
}

with DAG(
    dag_id="openweather_api_to_gcs",  # Unique identifier for this DAG
    default_args=default_args,  # Apply default settings to all tasks
    description="Fetch OpenWeather with Pandas+Requests in a venv, upload to GCS, trigger downstream DAG",
    schedule_interval=None,  # No automatic schedule; run manually via UI or API
    catchup=False,  # Don't backfill past dates
    tags=["weather", "gcs"],  # Tags for organizing DAGs in Airflow UI
) as dag:

    # -------------------------------------------------------------------
    # TASK 1: EXTRACT WEATHER DATA FROM OPENWEATHER API
    # Runs in isolated Python venv to avoid dependency conflicts
    # -------------------------------------------------------------------
    def _extract_openweather(api_key: str) -> str:
        """
        FUNCTION: Fetch weather forecast from OpenWeather API
        
        INPUT:
          - api_key: OpenWeather API key (provided via Airflow Variable)
        
        PROCESS:
          1. Call OpenWeather 5-day forecast API for Toronto
          2. Parse JSON response
          3. Flatten nested JSON into pandas DataFrame
          4. Convert DataFrame to CSV string
        
        OUTPUT:
          - Returns CSV string (passed to next task via XCom)
        """
        import requests
        import pandas as pd

        # OpenWeather API endpoint for 5-day forecast
        endpoint = "https://api.openweathermap.org/data/2.5/forecast"
        
        # API parameters: city & API key
        params = {"q": "Toronto,CA", "appid": api_key}

        # Make HTTP request and handle errors
        resp = requests.get(endpoint, params=params)
        resp.raise_for_status()  # Raise exception if request fails

        # Flatten nested JSON structure into a flat DataFrame
        # (OpenWeather returns nested "list" array with weather objects)
        df = pd.json_normalize(resp.json()["list"])
        print(df.head())  # Print first few rows for debugging
        
        # Convert DataFrame to CSV string (no index column)
        return df.to_csv(index=False)

    # Create the extract task using PythonVirtualenvOperator
    # (This runs Python code in an isolated virtual environment)
    extract_weather = PythonVirtualenvOperator(
        task_id="extract_weather_data",  # Unique task name
        python_callable=_extract_openweather,  # Function to execute
        
        # Install only required packages (pandas & requests)
        # Reduces venv setup time
        requirements=[
            "pandas==1.5.3",
            "requests==2.31.0",
        ],
        
        # Reuse system Python packages (numpy, etc.)
        # Avoids ABI (Application Binary Interface) compatibility issues
        system_site_packages=True,
        
        # Pass API key from Airflow Variable storage
        # Accessed via Airflow UI: Admin → Variables → openweather_api_key
        op_kwargs={"api_key": Variable.get("openweather_api_key")},
    )

    # -------------------------------------------------------------------
    # TASK 2: UPLOAD CSV DATA TO GOOGLE CLOUD STORAGE (GCS)
    # Retrieves data from Task 1 via XCom (Airflow cross-task communication)
    # -------------------------------------------------------------------
    def _upload_to_gcs(ds: str, **kwargs):
        """
        FUNCTION: Upload weather CSV to GCS bucket
        
        INPUT:
          - ds: Execution date in YYYY-MM-DD format (Airflow built-in)
          - **kwargs: Airflow context (includes task_instance 'ti')
        
        PROCESS:
          1. Retrieve CSV data from Task 1 (extract_weather_data) via XCom
          2. Create GCS hook for authentication
          3. Upload CSV file to bucket with date-based path
        
        OUTPUT:
          - File saved to: gs://weather-data-gds-my/weather/{date}/forecast.csv
        """
        # Get TaskInstance from Airflow context to access XCom
        ti = kwargs["ti"]
        
        # Pull CSV data from previous task's return value (stored in XCom)
        csv_data = ti.xcom_pull(task_ids="extract_weather_data")
        
        # Initialize GCS Hook for authentication & connection
        # Uses default GCP credentials from environment
        hook = GCSHook()
        
        # Upload CSV data to GCS
        hook.upload(
            bucket_name="weather-data-gds-my",  # GCS bucket name
            object_name=f"weather/{ds}/forecast.csv",  # File path with date
            data=csv_data,  # CSV string data to upload
            mime_type="text/csv",  # File type
        )

    # Create the upload task using standard PythonOperator
    # (Runs in Airflow worker environment, no isolated venv needed)
    upload_to_gcs = PythonOperator(
        task_id="upload_to_gcs",  # Unique task name
        python_callable=_upload_to_gcs,  # Function to execute
        op_kwargs={"ds": "{{ ds }}"},  # Pass execution date to function
    )

    # -------------------------------------------------------------------
    # TASK 3: TRIGGER DOWNSTREAM DAG FOR DATA TRANSFORMATION
    # Starts the transform DAG to clean, process, and load data to BigQuery
    # -------------------------------------------------------------------
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_data_transform_dag",  # Unique task name
        trigger_dag_id="transformed_weather_data_to_bq",  # DAG to trigger
        wait_for_completion=False,  # Don't wait for triggered DAG to finish
        # (Allows this DAG to complete faster, transform DAG runs independently)
    )

    # -------------------------------------------------------------------
    # DAG TASK DEPENDENCIES / WORKFLOW
    # -------------------------------------------------------------------
    # Define execution order: Extract → Upload → Trigger Transform
    # Each >> means "depends on" or "runs after"
    extract_weather >> upload_to_gcs >> trigger_transform