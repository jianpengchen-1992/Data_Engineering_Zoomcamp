from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# Define your combinations here!
PIPELINES = ["weather"]
CATEGORIES = ["forecast", "historical"] # Replace with actual categories

# 1. Define the DAG
with DAG(
    dag_id="daily_weather_ingestion",
    start_date=datetime(2019, 1, 1), # Sets the earliest possible date you can backfill
    schedule="0 8 * * *",            # Cron expression for 8:00 AM every day
    catchup=False,                   # IMPORTANT: Set to False so it doesn't try to run 5 years of data all at once!
    tags=["weather_data", "ingestion"],
) as dag:

    # 2. Dynamically generate the tasks using a loop
    for pipeline in PIPELINES:
        for cat in CATEGORIES:
            
            # Create a unique task ID for Airflow (e.g., "run_pipeline1_stromerzeugung")
            task_id = f"run_{pipeline}_{cat.lower()}"
            
            # Note the {{ ds }} - Airflow will inject the date here at runtime
            command = f"uv run python -m src.pipelines.{pipeline}.pipeline --start_date {{{{ ds }}}} --target_cat {cat}"
            
            # 3. Use the DockerOperator to spin up the container
            DockerOperator(
                task_id=task_id,
                # Replace with the actual name you used when building your image
                image="weather_pipeline_image:latest", 
                command=command,
                network_mode="bridge",
                # Automatically cleans up the container after it succeeds or fails
                auto_remove="force", 
                # Good practice: prevents permission issues with Airflow mounting temp files
                mount_tmp_dir=False, 
            )