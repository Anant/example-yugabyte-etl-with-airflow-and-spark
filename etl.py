import sys
import csv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import datediff, col, abs

spark = SparkSession\
    .builder\
    .appName("etl")\
    .getOrCreate()

# database = spark.conf.get("spark.database.name")
keyspace = spark.conf.get("spark.keyspace.name")

spark.conf.set("spark.sql.catalog.yugabyte",
               "com.datastax.spark.connector.datasource.CassandraCatalog")

spark.sql(f"use yugabyte.{keyspace}")

calcDF = spark.sql(
    "select job_title, employee_id, employee_name, abs(datediff(last_day, first_day)) as days_worked from previous_employees_by_job_title")

calcDF.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="days_worked_by_previous_employees_by_job_title", keyspace=keyspace)\
    .save()

spark.stop()