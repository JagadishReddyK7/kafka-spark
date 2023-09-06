from pyspark.sql import SparkSession
from generate_upload_csv import write_csv_files,upload_csv_files_to_s3
import os
import boto3
import json

session = boto3.Session(
    aws_access_key_id='AKIAZ7A2D7Q6BQDZDGYR',
    aws_secret_access_key='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'
)
s3 = session.resource('s3')

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

spark = SparkSession.builder.appName("kafka_with_spark").getOrCreate()
df=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","mallCustomers").load()

json_array=df.toJSON().collect()
json_str=json.dumps(json_array)
write_csv_files(json_str,'csv_files')

local_directory='csv_files'
bucket_name = 'kafka-s3-connection-bucket'
upload_csv_files_to_s3(local_directory,bucket_name,'sample_csv1')