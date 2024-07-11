import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from garmin_api import login, all_activities_to_file, download_gpx, load_gpx_to_db

with DAG(
        dag_id="garmin_dag",
        start_date=datetime.datetime(2024, 5, 27),
        schedule_interval="@daily",
        catchup=False,
):
    login = PythonOperator(
        task_id="login_to_garmin",
        python_callable=login,
    )
    all_activities_to_file = PythonOperator(
        task_id="load_activities",
        python_callable=all_activities_to_file,
    )
    download_gpx = PythonOperator(
        task_id="download_gpx",
        python_callable=download_gpx,
    )
    load_gpx_to_db = PythonOperator(
        task_id="load_gpx_to_db",
        python_callable=load_gpx_to_db,
    )

    login >> all_activities_to_file >> download_gpx >> load_gpx_to_db

