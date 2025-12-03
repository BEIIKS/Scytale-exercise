from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago

# --- ייבוא לוגיקת ה-ETL מתוך תיקיית ה-scripts ---
# הנחה: הלוגיקה שלך נמצאת בקבצים נפרדים בתיקיית 'scripts'
from src.extract_data import fetch_pr_data
from src.transform_data import transform_data_logic
from src.load_data import load_data_logic

# --- 1. הגדרות DAG כלליות ---
default_args = {
    "owner": "scytale_data_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,  # הגדלת רטרי (Retries) למקרה של כשל API רגעי
    "retry_delay": timedelta(minutes=1),
}
with DAG(
        dag_id="compliance_monitoring_pipeline",
        default_args=default_args,
        schedule_interval="0 0 * * *",
        start_date=days_ago(1),
        catchup=False,
        tags=["github", "compliance", "etl"],
) as dag:
    extract_task = PythonOperator(
        task_id="fetch_pr_data",
        python_callable=fetch_pr_data,
        op_kwargs={
            'output_dir': '/opt/airflow/data',
            'execution_date': '{{ ds }}'
        },
    )
    transform_task = PythonOperator(
        task_id="transform_github_data",
        python_callable=transform_data_logic,
        op_kwargs={
            'input_file_path': "{{ task_instance.xcom_pull(task_ids='fetch_pr_data') }}",
            'execution_date': '{{ ds }}'
        },
    )

    load_to_parquet = PythonOperator(
        task_id="load_to_parquet",
        python_callable=load_data_logic,
        op_kwargs={
            'input_filepath': f"{{{{ ti.xcom_pull(task_ids='transform_github_data') }}}}",

            'output_dir': '/opt/airflow/data/final',
            'execution_date': '{{ ds }}',
        }
    )

extract_task >> transform_task >> load_to_parquet