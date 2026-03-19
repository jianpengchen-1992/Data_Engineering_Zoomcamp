from airflow.providers.docker.operators.docker import DockerOperator
import shlex
# 1. Define the DAG
def create_ingestion_task(pipeline, target_main_cat, target_sub_cat = None, pool_name=None):
# Create a unique task ID for Airflow (e.g., "run_pipeline1_stromerzeugung")
    task_id = f"run_{pipeline}_{target_main_cat.lower()}_{target_sub_cat.lower()}" if target_sub_cat else f"run_{pipeline}_{target_main_cat.lower()}"
    task_id = task_id.replace(" ", "_")  # Replace spaces with underscores for valid task IDs

    safe_main_cat = shlex.quote(target_main_cat) # Safely quote the main category for shell command
    if target_sub_cat:
        safe_sub_cat = shlex.quote(target_sub_cat) # Safely quote the sub category for shell command
    
    if pipeline == 'energy':
        command = f"uv run python -m src.pipeline.{pipeline}.pipeline --start_date $START_DATE --target_main_cat {safe_main_cat} --target_sub_cat {safe_sub_cat}"
    elif pipeline == 'weather':
        command = f"uv run python -m src.pipeline.{pipeline}.pipeline --start_date $START_DATE --target_main_cat {safe_main_cat}"
    else:
        raise ValueError(f"Unsupported pipeline: {pipeline}")
    environment={
        "START_DATE": "{{ ds }}",  # Airflow's execution date
    }
    print(f"Creating task with ID: {task_id} and command: {command}")
    return DockerOperator(
        task_id=task_id,
        image="pipeline_image:latest",
        command=command,
        pool=pool_name,
        network_mode="bridge",
        auto_remove="force",
        mount_tmp_dir=False,
        environment=environment,
    )