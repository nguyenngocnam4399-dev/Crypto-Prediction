from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crawl_crypto_news",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    crawl = BashOperator(
        task_id="crawl_news",
        bash_command="/root/airflow_venv/bin/python /root/crypto_ubuntu/crawl.py"
    )

    spark_news_coin = BashOperator(
        task_id="spark_news_coin_mapping",
        bash_command="""
        /opt/spark/bin/spark-submit \
        /root/crypto_ubuntu/spark_job_news_coin_mapping.py
        """
    )

    crawl >> spark_news_coin