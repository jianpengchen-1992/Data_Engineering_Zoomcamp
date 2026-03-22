from airflow import DAG
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dags.utils.task_factory import create_ingestion_task

# DAG 1: Historical
with DAG('weather_historical', 
         start_date=datetime(2019, 1, 1), 
         schedule="0 8 * * *", 
         catchup=False,
         tags=["weather", "historical"]
         ) as dag_hist:
    
    hist_task = create_ingestion_task(
        pipeline='weather', 
        target_main_cat='historical',
    )

# DAG 2: Forecast
with DAG('weather_forecast', start_date=datetime(2022, 1, 1), 
         schedule="0 8 * * *", 
         catchup=False,
         tags=["weather", "forecast"]
         ) as dag_fore:
    
    fore_task = create_ingestion_task(
        pipeline='weather', 
        target_main_cat='forecast',
    )