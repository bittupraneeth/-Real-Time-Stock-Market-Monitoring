from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Set timezone offset manually (IST = UTC+5:30)
IST_START = 13  # 7:00 PM IST = 13:30 UTC approx
IST_END = 20    # 1:30 AM IST = 20:00 UTC

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'spark_streaming_evening_job',
    default_args=default_args,
    schedule_interval='*/30 * * * *',  # every 30 minutes
    catchup=False,
)

def is_within_time_range(execution_time):
    hour = execution_time.hour
    return IST_START <= hour <= IST_END

with dag:
    from airflow.operators.python import ShortCircuitOperator

    check_time = ShortCircuitOperator(
        task_id='check_execution_window',
        python_callable=lambda: is_within_time_range(datetime.utcnow()),
    )

    spark_submit_task = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/opt/spark_jobs/my_streaming_job.py',
        conn_id='spark_default',
        conf={"spark.executor.memory": "2g", "spark.driver.memory": "1g"},
        application_args=["--env", "prod"],
        dag=dag,
    )

    check_time >> spark_submit_task
