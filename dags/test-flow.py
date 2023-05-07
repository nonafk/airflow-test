from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.utils import timezone
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator


now = datetime.now()

default_args = {
    "owner": "airflow",
    "email": ["recipient@example.com"],
    "start_date": timezone.datetime(now.year, now.month, now.day),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


def my_function():
    raise Exception("Something went wrong")


with DAG(
    'example_scheduler_dag',
    default_args=default_args,
    description='Example DAG with retries',
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:

    my_operator = PythonOperator(
        task_id='example_task',
        python_callable=my_function,
        dag=dag
    )

    notify = EmailOperator(
        task_id="notify",
        to=["recipient@example.com"],
        subject="Loaded data  {{ ds }}",
        html_content="Your pipeline has loaded data into database successfully",
    )

    my_operator >> notify
