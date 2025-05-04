## Intelligent IoT RealTime Structured Stream Processing 
Project to simulate the movement of a vehicle from one city location to another. 
The information collected about the vehicle at each snapshot in time mmimics IoT devices with functions created to represent each such device. We have modelled functions for Vehicle Information, GPS Information, Traffic Camera Information, Weather Information and finally Emergency Information.
The streaming data is handled by Apache Kafka and Zookeeper with Apache Spark consuming data from the Kafka topic.
The data is read from the Kafka topics which represent the IoT devices for collecting vehicle, emergency, traffic, weather and GPS data
and written to S3 in a streaming fashion.

## config.py
We store our secrets like AWS access keys in this file as a dictionary and reference the secret by importing the config file

## main.py
This python script models the movement of a vehicle from London to Birmingham and captures the supplementary information around weather, emergency data, etc and writes the data to a kafka topic

# spark-area.py
This script uses Pyspark to read the data from the Kafka topics into a Spark dataframe.
The data is read from the Kafka topics and the dataframes are written to S3 as parquet files

## S3 and AWS Glue 
The next step, further downstream the data pipeline is where AWS Glue is used to Crawl the S3 buckets and create a Glue Data Catalog.
We create a single Glue crawler that runs over all the subfolders within our main S3 bucket. 
This will create the tables within the data catalog for the dataframes created for Vehicle Data, GPS Data, Traffic Camera Data, Weather Data and Emergency Data.

## Athena and Redshift
S3 Tables can be queried using standard SQL in Athena or alternatively create Redshift Cluster 
Associate IAM role to redshift cluster that allows Redshift read from S3
Once the cluster is created and has been associated with the relevant S3 bucket where the data is stored,
Simply connect to the Redshift JDBC endpoint from DBeaver and update the relevant jdbc driver.