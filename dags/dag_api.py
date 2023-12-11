from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='My DAG to run Docker container with scripts',
    schedule_interval='0 5 * * *',  # 5 ранку кожного дня
)

scripts_directory = "/scripts"

run_scripts_task = DockerOperator(
    task_id='run_script_task',
    image='your_image_name:your_image_tag',  # Вкажи image name і tag  
    api_version='auto',
    auto_remove=True,
    command=["python", f"{scripts_directory}/001_installs_mart.py", "python", f"{scripts_directory}/002_costs_mart.py",\
             "python", f"{scripts_directory}/003_orders_mart.py", "python", f"{scripts_directory}/004_events_mart.py"],
    dag=dag,
)

run_scripts_task 