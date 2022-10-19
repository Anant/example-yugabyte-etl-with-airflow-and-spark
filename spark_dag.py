from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

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

    JOB_CONF = {
        "spark.sql.extensions": "com.datastax.spark.connector.CassandraSparkExtensions",
        "spark.keyspace.name": "demo",
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

    load_and_write
    # etl_job
    # load_and_write >> etl_job