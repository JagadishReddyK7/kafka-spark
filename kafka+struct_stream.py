from pyspark.sql import SparkSession
import os
import boto3
import io
import pickle

session = boto3.Session(
    aws_access_key_id='AKIAZ7A2D7Q6BQDZDGYR',
    aws_secret_access_key='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'
)
s3 = session.resource('s3')

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

spark = SparkSession.builder.appName("kafka_with_spark").getOrCreate()
df=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","mallCustomers").load()
df_bytes=pickle.dumps(df)

s3.Object('kafka-s3-connection-bucket','stream.txt').upload_fileobj(io.BytesIO(df_bytes))

query = df.writeStream.outputMode("append").format("console").start()

query.awaitTermination()