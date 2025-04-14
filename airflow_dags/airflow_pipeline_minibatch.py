# Main


from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 11, 4, 0), 
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ooni_data_pipeline",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    catchup=False,
    description="Run OONI Spark jobs sequentially and load data into Elasticsearch + S3",
) as dag:

    run_telegram = BashOperator(
        task_id="run_telegram_job",
        bash_command="/opt/spark/bin/spark-submit /opt/airflow/dags/scripts/telegram_job.py"
    )

    run_whatsapp = BashOperator(
        task_id="run_whatsapp_job",
        bash_command="/opt/spark/bin/spark-submit /opt/airflow/dags/scripts/whatsapp_job.py"
    )

    run_signal = BashOperator(
        task_id="run_signal_job",
        bash_command="/opt/spark/bin/spark-submit /opt/airflow/dags/scripts/signal_job.py"
    )

    run_facebook = BashOperator(
        task_id="run_facebook_job",
        bash_command="/opt/spark/bin/spark-submit /opt/airflow/dags/scripts/facebook_job.py"
    )

    run_telegram >> run_whatsapp >> run_signal >> run_facebook