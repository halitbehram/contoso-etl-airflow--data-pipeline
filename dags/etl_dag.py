from airflow import DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import logging
import sys
import os
import pandas as pd
import pickle

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etl.extract.extract import extract_selected_tables
from etl.transform.main import CustomerAnalytics
from etl.load.loader import (save_csv, save_excel, save_multiple_csv, save_multiple_excel, save_sqlite)

logger = logging.getLogger(__name__)

def get_shared_dir():
    shared_dir = Path("/opt/airflow/shared/contoso-etl-airflow")
    shared_dir.mkdir(parents=True, exist_ok=True)
    return shared_dir


def extract_data():
    shared_dir = get_shared_dir()

    dfs = extract_selected_tables()
    with open(shared_dir / "dfs.pkl", "wb") as f:
        pickle.dump(dfs, f)

    logger.info("✅ Veriler alındı")

def transform_data():
    shared_dir = get_shared_dir()

    with open(shared_dir / "dfs.pkl", "rb") as f:
        dfs = pickle.load(f)

    if dfs is None:
        logger.error("Veri bulunamadı!")
        raise ValueError("Veri bulunamadı!")

    analytics = CustomerAnalytics(dfs)
    result = {
    "rfm": analytics.rfm(),
    "churn": analytics.churn(),
    "cohort": analytics.cohort()
    }
    result2 = analytics.never_purchased_customers()
    with open(shared_dir / "result.pkl", "wb") as f:
        pickle.dump(result, f)
    with open(shared_dir / "result2.pkl", "wb") as f:
        pickle.dump(result2, f)
    
    logger.info("✅ Veriler dönüştürüldü")

def load_data():
    shared_dir = get_shared_dir()

    with open(shared_dir / "result.pkl", "rb") as f:
        load_data1= pickle.load(f)
    with open(shared_dir / "result2.pkl", "rb") as f:
        load_data2= pickle.load(f)
    
    if load_data1 is None or load_data2 is None:
        logger.error("Veri bulunamadı!")
        raise ValueError("Veri bulunamadı!")
    
    today = datetime.today().strftime("%Y-%m-%d")
    base_output_dir = Path("/opt/airflow/output/contoso-etl-airflow")                   
    output_dir = base_output_dir / today
    output_dir.mkdir(parents=True, exist_ok=True)

    file1 = output_dir / f"rfm-churn-geo.xlsx"
    file2 = output_dir / f"never-purchased-customers.csv"

    save_multiple_excel(load_data1, file1)
    save_csv(load_data2, file2)

    logger.info(f"✅ Veriler başarıyla kaydedildi:\n - {file1}\n - {file2}")

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['htorunlu@gmail.com'],  
}

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data_task",
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id="load_data_task",
        python_callable=load_data
    )

    
    extract_task >> transform_task >> load_task