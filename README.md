# Yugabyte ETL with Airflow and Spark

In this walkthrough, we will cover how we can use [Airflow](https://airflow.apache.org/) to trigger [Spark](https://spark.apache.org/) ETL jobs that move date into and within [Yugabyte](https://www.yugabyte.com/). This demo will be relatively simple; however, it can be expanded upon with the addition of other technologies like Kafka, setting scheduling on the Spark jobs to make it a concurrent process, or in general creating more complex Yugabyte ETL pipelines. We will focus on showing you how to connect Airflow, Spark, and Yugabyte, and in our case today, dockerized Yugabyte. The reason we are using dockerized Yugabyte is because we want everyone to be able to do this demo without having to worry about OS incompatibilities and the sort. For that reason, we will also be using [Gitpod](https://gitpod.io/)

For this walkthrough, we will use 2 Spark jobs. The first Spark job will load 100k rows from a CSV and then write it into a Yugabyte table. The second Spark job will read the data from the prior Yugabyte table, do some transformations, and then write the transformed data into a different Yugabyte table. We also used PySpark to reduce the number of steps to get this working. If we used Scala, we would be required to build the JAR's and that would require more time.

If you have not already opened this in gitpod, then `CTR + Click` the button below and get started! <br></br>
[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io#https://github.com/Anant/example-yugabyte-etl-with-airflow-and-spark)

## 1. Set-up Dockerized Yugabyte

### 1.1 - Run the below command and make the ports public
```bash
docker run -d --name yugabyte  -p7000:7000 -p9000:9000 -p5433:5433 -p9042:9042\
 yugabytedb/yugabyte:2.15.2.0-b87 bin/yugabyted start\
 --daemon=false
```

### 1.2 - Set up Yugabyte Tables
```bash
docker exec -it yugabyte ycqlsh -e "$(cat setup.cql)"
```

## 2. Set up Airflow

We will be using the quick start script that Airflow provides [here](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html).

```bash
bash setup.sh
```

## 3. Start Spark in standalone mode

### 3.1 - Start master

```bash
./spark-3.2.2-bin-hadoop3.2/sbin/start-master.sh
```
The reason we are using spark 3.2.2 is because we run into `Unsupported data source V2 partitioning type: CassandraPartitioning` errors when using spark 3.3.X.

### 3.2 - Start worker

Open port 8080 in the browser, copy the master URL, and paste in the designated spot below

```bash
./spark-3.2.2-bin-hadoop3.2/sbin/start-worker.sh <master-URL>
```

## 4. Move spark_dag.py to ~/airflow/dags

### 4.1 - Create ~/airflow/dags

```bash
mkdir ~/airflow/dags
```

### 4.2 - Move spark_dag.py

```bash
mv spark_dag.py ~/airflow/dags
```

## 5, Open port 8081 to see Airflow UI and check if `example_yugabyte_etl` exists. 
If it does not exist yet, give it a few seconds to refresh.

## 7. Update Spark Connection and unpause the `example_yugabyte_etl`.

### 7.1 - Under the `Admin` section of the menu, select `spark_default` and update the host to the Spark master URL. Save once done.

### 7.2 - Select the `DAG` menu item and return to the dashboard. Unpause the `example_yugabyte_etl`, and then click on the `example_yugabyte_etl`link. 

## 8. Trigger the DAG from the tree view and click on the graph view afterwards

## 9. Confirm data in Yugabyte

### 9.1 - Check `previous_employees_by_job_title`

```bash
docker exec -it yugabyte ycqlsh -e "select * from demo.previous_employees_by_job_title where job_title='Dentist';"
```

### 9.2 - Check `days_worked_by_previous_employees_by_job_title`
```bash
docker exec -it yugabyte ycqlsh -e "select * from demo.days_worked_by_previous_employees_by_job_title where job_title='Dentist';"
```

And that will wrap up our walkthrough. Again, this is a introduction on how to set up a basic Yugabyte ETL process run by Airflow and Spark. As mentioned above, these baby steps can be used to further expand and create more complex and scheduled / repeated Yugabyte ETL processes run by Airflow and Spark.
