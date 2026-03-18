from airflow.providers.docker.operators.docker import DockerOperator
# 1. Define the DAG
def create_ingestion_task(pipeline, target_main_cat, target_sub_cat = None, pool_name=None):
# Create a unique task ID for Airflow (e.g., "run_pipeline1_stromerzeugung")
    task_id = f"run_{pipeline}_{target_main_cat.lower()}_{target_sub_cat.lower()}" if target_sub_cat else f"run_{pipeline}_{target_main_cat.lower()}"
    task_id = task_id.replace(' ', '_')
    
    # Note the {{ ds }} - Airflow will inject the date here at runtime
    if target_sub_cat:
        command = f"uv run python -m src.pipelines.{pipeline}.pipeline --start_date {{{{ ds }}}} --target_main_cat {target_main_cat} --target_sub_cat {target_sub_cat}"
    else:
        command = f"uv run python -m src.pipelines.{pipeline}.pipeline --start_date {{{{ ds }}}} --target_main_cat {target_main_cat}"
    
    # 3. Use the DockerOperator to spin up the container
    DockerOperator(
        task_id=task_id,
        # Replace with the actual name you used when building your image
        image="pipeline_image:latest", 
        command=command,
        pool = pool_name,  # Optional: specify an Airflow pool to limit concurrency
        network_mode="bridge",
        # Automatically cleans up the container after it succeeds or fails
        auto_remove="force", 
        # Good practice: prevents permission issues with Airflow mounting temp files
        mount_tmp_dir=False, 
    )