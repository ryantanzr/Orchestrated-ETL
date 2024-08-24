from internal.pipeline import extract_from_csv, transformation, load_to_warehouse

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup

with DAG(
        "Running_Data_ETL_Pipeline",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=3),
        },
        description="DAG for orchestrating ETL pipeline",
        schedule=timedelta(minutes=3),
        start_date=datetime.now(),
        catchup=False,
        tags=["ETL"],
        ) as dag:
    
    with TaskGroup("ETL_2019") as ETL_2019:

        with TaskGroup("ETL_daily_2019", tooltip="ETL for daily data in 2019") as ETL_daily_2019:
            extract = extract_from_csv("run_ww_2019_d.csv")
            transform = transformation(extract)
            load = load_to_warehouse(transformation, "run_ww_2019_d_transformed.csv")
        
        with TaskGroup("ETL_weekly_2019", tooltip="ETL for weekly data in 2019") as ETL_weekly_2019:
            extract = extract_from_csv("run_ww_2019_w.csv")
            transform = transformation(extract)
            load = load_to_warehouse(transformation, "run_ww_2019_w_transformed.csv")

        with TaskGroup("ETL_monthly_2019", tooltip="ETL for monthly data in 2019") as ETL_monthly_2019:
            extract = extract_from_csv("run_ww_2019_m.csv")
            transform = transformation(extract)
            load = load_to_warehouse(transformation, "run_ww_2019_m_transformed.csv")

        with TaskGroup("ETL_quarterly_2019", tooltip="ETL for quarterly data in 2019") as ETL_quarterly_2019:
            extract = extract_from_csv("run_ww_2019_q.csv")
            transform = transformation(extract)
            load = load_to_warehouse(transformation, "run_ww_2019_q_transformed.csv")
    
    with TaskGroup("ETL_2020") as ETL_2020:

        with TaskGroup("ETL_daily_2020", tooltip="ETL for daily data in 2020") as ETL_daily_2020:
            extract = extract_from_csv("run_ww_2020_d.csv")
            transform = transformation(extract)
            load = load_to_warehouse(transformation, "run_ww_2020_d_transformed.csv")
        
        with TaskGroup("ETL_weekly_2020", tooltip="ETL for weekly data in 2020") as ETL_weekly_2020:
            extract = extract_from_csv("run_ww_2020_w.csv")
            transform = transformation(extract)
            load = load_to_warehouse(transformation, "run_ww_2020_w_transformed.csv")
        
        with TaskGroup("ETL_monthly_2020", tooltip="ETL for monthly data in 2020") as ETL_monthly_2020:
            extract = extract_from_csv("run_ww_2020_m.csv")
            transform = transformation(extract)
            load = load_to_warehouse(transformation, "run_ww_2020_m_transformed.csv")

        with TaskGroup("ETL_quarterly_2020", tooltip="ETL for quarterly data in 2020") as ETL_quarterly_2020:
            extract = extract_from_csv("run_ww_2020_q.csv")
            transform = transformation(extract)
            load = load_to_warehouse(transformation, "run_ww_2020_q_transformed.csv")

    ETL_2019 >> ETL_2020
