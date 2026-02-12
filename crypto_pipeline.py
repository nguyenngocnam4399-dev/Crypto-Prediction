from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="crypto_to_fact",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 * * * *",   # mỗi giờ
    catchup=False,
    tags=["crypto", "spark", "indicator"]
) as dag:

    # =================================================
    # RAW -> STAGING
    # =================================================
    raw_to_staging = BashOperator(
        task_id="raw_to_staging",
        bash_command="""
        /opt/spark/bin/spark-submit \
        --conf spark.airflow.execution_date="{{ ds }}" \
        /root/crypto_ubuntu/raw_to_staging.py
        """
    )

    # =================================================
    # STAGING -> FACT
    # =================================================
    staging_to_fact = BashOperator(
        task_id="staging_to_fact",
        bash_command="""
        /opt/spark/bin/spark-submit \
        --packages mysql:mysql-connector-java:8.0.33 \
        /root/crypto_ubuntu/staging_to_fact.py
        """
    )

    # =================================================
    # BTC PIPELINE
    # =================================================
    with TaskGroup(group_id="btc") as btc_group:

        indicator_btc = BashOperator(
            task_id="indicator",
            bash_command="""
            /opt/spark/bin/spark-submit \
            --packages mysql:mysql-connector-java:8.0.33 \
            /root/crypto_ubuntu/Crypto/indicator_engine/main.py 1 1
            """
        )

        metric_value_btc = BashOperator(
            task_id="metric_value",
            bash_command="""
            /opt/spark/bin/spark-submit \
            --packages mysql:mysql-connector-java:8.0.33 \
            /root/crypto_ubuntu/Crypto/indicator_engine/1_spark_job_metric_value_btc.py
            """
        )

        prediction_btc = BashOperator(
            task_id="spark_job_prediction_fact",
            bash_command="""
            /opt/spark/bin/spark-submit \
            --packages mysql:mysql-connector-java:8.0.33 \
            /root/crypto_ubuntu/Crypto/indicator_engine/1_prediction_engine_btc.py
            """
        )

        confirmation_btc = BashOperator(
            task_id="spark_job_prediction_confirm_tp_sl_btc",
            bash_command="""
            /opt/spark/bin/spark-submit \
            --packages mysql:mysql-connector-java:8.0.33 \
            /root/crypto_ubuntu/Crypto/indicator_engine/1_backtest_engine_btc.py
            """
        )
        indicator_btc >> metric_value_btc >> prediction_btc >> confirmation_btc

    # =================================================
    # ETH PIPELINE
    # =================================================
    with TaskGroup(group_id="eth") as eth_group:

        indicator_eth = BashOperator(
            task_id="indicator",
            bash_command="""
            /opt/spark/bin/spark-submit \
            --packages mysql:mysql-connector-java:8.0.33 \
            /root/crypto_ubuntu/Crypto/indicator_engine/main.py 2 1
            """
        )

        metric_value_eth = BashOperator(
            task_id="metric_value",
            bash_command="""
            /opt/spark/bin/spark-submit \
            --packages mysql:mysql-connector-java:8.0.33 \
            /root/crypto_ubuntu/Crypto/indicator_engine/1_spark_job_metric_value_eth.py
            """
        )

        prediction_eth = BashOperator(
            task_id="spark_job_prediction_fact",
            bash_command="""
            /opt/spark/bin/spark-submit \
            --packages mysql:mysql-connector-java:8.0.33 \
            /root/crypto_ubuntu/Crypto/indicator_engine/1_prediction_engine_eth.py
            """
        )

        confirmation_eth = BashOperator(
            task_id="spark_job_prediction_confirm_tp_sl_eth",
            bash_command="""
            /opt/spark/bin/spark-submit \
            --packages mysql:mysql-connector-java:8.0.33 \
            /root/crypto_ubuntu/Crypto/indicator_engine/1_backtest_engine_eth.py
            """
        )
        indicator_eth >> metric_value_eth >> prediction_eth >> confirmation_eth

    # =================================================
    # BNB PIPELINE
    # =================================================
    with TaskGroup(group_id="bnb") as bnb_group:

        indicator_bnb = BashOperator(
            task_id="indicator",
            bash_command="""
            /opt/spark/bin/spark-submit \
            --packages mysql:mysql-connector-java:8.0.33 \
            /root/crypto_ubuntu/Crypto/indicator_engine/main.py 3 1
            """
        )

        metric_value_bnb = BashOperator(
            task_id="metric_value",
            bash_command="""
            /opt/spark/bin/spark-submit \
            --packages mysql:mysql-connector-java:8.0.33 \
            /root/crypto_ubuntu/Crypto/indicator_engine/1_spark_job_metric_value_bnb.py
            """
        )

        prediction_bnb = BashOperator(
            task_id="spark_job_prediction_fact",
            bash_command="""
            /opt/spark/bin/spark-submit \
            --packages mysql:mysql-connector-java:8.0.33 \
            /root/crypto_ubuntu/Crypto/indicator_engine/1_prediction_engine_bnb.py
            """
        )

        confirmation_bnb = BashOperator(
            task_id="spark_job_prediction_confirm_tp_sl_bnb",
            bash_command="""
            /opt/spark/bin/spark-submit \
            --packages mysql:mysql-connector-java:8.0.33 \
            /root/crypto_ubuntu/Crypto/indicator_engine/1_backtest_engine_bnb.py
            """
        )
        indicator_bnb >> metric_value_bnb >> prediction_bnb >> confirmation_bnb

    fp_growth = BashOperator(
        task_id="spark_job_fp_growth",
        bash_command="""
        /opt/spark/bin/spark-submit \
        --conf spark.pyspark.python=/root/crypto_ubuntu/venv/bin/python \
        /root/crypto_ubuntu/spark_job_fp_growth.py
        """
    )
    # =================================================
    # DAG ORDER
    # =================================================
    raw_to_staging >> staging_to_fact
    staging_to_fact >> [btc_group, eth_group, bnb_group] >> fp_growth
