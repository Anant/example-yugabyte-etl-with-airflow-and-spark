from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

import os
import sys

args = {
    'owner': 'Airflow',
}

with DAG(
    dag_id='example_yugabyte_etl',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    # need the below to resolve py4j.protocol.Py4JJavaError: An error occurred while calling o32.load errors
    os.environ['SPARK_HOME'] = '/workspace/example-yugabyte-etl-with-airflow-and-spark/spark-3.2.2-bin-hadoop2.7'
    sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

    JOB_CONF = {
        "spark.sql.extensions": "com.datastax.spark.connector.CassandraSparkExtensions",
        "spark.keyspace.name": "demo",
        "spark.cassandra.connection.host": "127.0.0.1"
    }

    load_and_write = SparkSubmitOperator(
        task_id="load_and_write_job",
        application="/workspace/example-yugabyte-etl-with-airflow-and-spark/extract_and_load.py",
        conf=JOB_CONF,
        packages="com.yugabyte.spark:spark-cassandra-connector_2.12:3.2-yb-2"
    )

    etl_job = SparkSubmitOperator(
        task_id="etl_job",
        application="/workspace/example-yugabyte-etl-with-airflow-and-spark/etl.py",
        conf=JOB_CONF,
        packages="com.yugabyte.spark:spark-cassandra-connector_2.12:3.2-yb-2"
    )

    load_and_write >> etl_job
