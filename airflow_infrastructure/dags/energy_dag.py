from airflow import DAG
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dags.utils.task_factory import create_ingestion_task

with DAG('daily_energy_ingestion', 
         start_date=datetime(2019, 1, 1), 
         schedule="0 8 * * *", 
         catchup=False,
         tags=["energy_data", "ingestion"], #set some tags to make it easier to find in the Airflow UI
         ) as dag:
    
    # Just stamp out the tasks! No massive blocks of repeated Docker config.
    forecast_cons = create_ingestion_task('energy', 'Stromverbrauch', 'Prognostizierter Stromverbrauch')
    actual_cons   = create_ingestion_task('energy', 'Stromverbrauch', 'Realisierter Stromverbrauch')
    forecast_gen  = create_ingestion_task('energy', 'Stromerzeugung', 'Prognostizierte Erzeugung')
    actual_gen    = create_ingestion_task('energy', 'Stromerzeugung', 'Realisierte Erzeugung')

    # They run in parallel automatically!
    forecast_cons >> actual_cons
    forecast_gen >> actual_gen

