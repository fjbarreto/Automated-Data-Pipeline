# Automated Data Pipeline: Data Engineering Nanodegree Udacity Project

Music streaming company, **Sparkify**, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow. My job is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

## Data

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to. Here are the s3 links for each:

### Song Data

````s3://udacity-dend/song_data````

### Log Data 

````s3://udacity-dend/log_data````

## Pîpeline Overview

Design a DAG (directed acyclic graph) that runs hourly with your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data quality with **Apache Airflow**. The DAG should look something like this:

![DAGGGG](https://user-images.githubusercontent.com/97537153/190595038-e2514c10-0d63-4462-b61f-55bb7cad8944.png)

## Files

• **dags/udac_example_dag.py**: Python script with code that calls operator plugins and generates DAG. 

• **dags/create_tables.sql**: SQL queries used to ````CREATE```` tables on **Redshift** cluster.

• **plugins/operators/stage_redshift.py**: Custom Python operator plugin created to ````COPY```` song and log data from S3 to Redshift cluster. 

• **plugins/operators/load_fact.py**: Custom Python operator plugin created to ````INSERT```` data to fact table **songplays**.

• **plugins/operators/load_dimensions.py**: Custom Python operator plugin created to ````INSERT```` data into dimension tables of the star schema database.

• **plugins/operators/data_quality.py**: Custom Python operator plugin used to check data quality of created tables.

• **plugins/helpers/sql_queries.py**: SQL statements used to ````SELECT```` data we need to ````INSERT```` with **load_dimensions.py** and **load_facts.py**.

## Prerequisites

• Create an IAM User in AWS.

• Create a redshift cluster in AWS.

• Connect Airflow and AWS.

• Connect Airflow to the AWS Redshift Cluster.



