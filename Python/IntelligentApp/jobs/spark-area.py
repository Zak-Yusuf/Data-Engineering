# Here we will write our spark jobs to listen to events from the Kafka topic and process them
# We will write apache spark script here

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import col, from_json
from config import configuration

# This script is used to read data from Kafka topics and write it to S3 as parquet files
# The data is read from the Kafka topics which represent the IoT devices for collecting vehicle, emergency, traffic, weather and GPS data
#  and written to S3 in a streaming fashion

def main():
    # configure maven jars for Spark to connect to Kafka and AWS S3
    spark = SparkSession.builder.appName("IntelligentAppStreaming")\
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk:1.11.469")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))\
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .getOrCreate()

    # Adjust the log level to minimize the console output
    spark.sparkContext.setLogLevel('WARN')

    #  Vehicle Schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True), 
    ])

    # GPS Schema
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True),    
    ])   

    # Traffic Schema
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("snapshot", StringType(), True),    
    ])

    # Weather Schema
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),  
        StructField("airQualityIndex", DoubleType(), True),    
    ])

    # Emergency Schema
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("incidentType", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("incidentStatus", StringType(), True),
        StructField("severity", StringType(), True),  
        StructField("description", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        # watermark allows us to set duration for how long we want to keep the data in memory
        # for example, if we set it to 2 minutes, then the data older than 2 minutes will be removed from memory
        return (spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'broker:29092')
            .option('subscribe', topic)
            .option('startingOffsets', 'earliest')
            .load()
            .selectExpr('CAST(value AS STRING)')
            .select(from_json(col('value'), schema).alias('data'))
            .select('data.*')
            .withWatermark('timestamp', '2 minutes')
            )

    # function to write the stream to S3 as a parquet file using the checkpoint folder
    # checkpoint folder is used to store the state of the stream so that it can be resumed in case of failure
    def streamWriter(DataFrame, checkpointFolder, output):
       return(input.writeStream
            .format('parquet')
            .option('checkpointLocation', checkpointFolder)
            .option('path', output)
            .outputMode('append')
            .start())
    

    # Read from Kafka topics
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    # write dataframes to S3 as parquet files
    # the checkpoint folder is used to store the state of the stream so that it can be resumed in case of failure
    query1 = streamWriter(vehicleDF, 's3a://intelligent-spark-streaming-data/checkpoints/vehicle_data', 
                's3a://intelligent-spark-streaming-data/data/vehicle_data')
    query2 = streamWriter(gpsDF, 's3a://intelligent-spark-streaming-data/checkpoints/gps_data', 
                's3a://intelligent-spark-streaming-data/data/gps_data')
    query3 = streamWriter(trafficDF, 's3a://intelligent-spark-streaming-data/checkpoints/traffic_data', 
                's3a://intelligent-spark-streaming-data/data/traffic_data')
    query4 = streamWriter(weatherDF, 's3a://intelligent-spark-streaming-data/checkpoints/weather_data', 
                's3a://intelligent-spark-streaming-data/data/weather_data')
    query5 = streamWriter(emergencyDF, 's3a://intelligent-spark-streaming-data/checkpoints/emergency_data', 
                's3a://intelligent-spark-streaming-data/data/emergency_data')

    query5.awaitTermination()

if __name__ == "__main__":
    main()