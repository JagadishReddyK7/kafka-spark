from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType,StructField,StringType
import os
import json
import boto3

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

checkpoint_location='s3a://datalake-store-poc/raw/checkpoints'

spark = SparkSession.builder.config('spark.master','local').\
        config("spark.sql.streaming.checkpointLocation", checkpoint_location).\
        config('spark.jars.packages','org.apache.hadoop:hadoop-aws:3.3.5,org.apache.hadoop:hadoop-common:3.3.5').\
        config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider').\
        config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").\
        getOrCreate()

sc=spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA3JCX6CPUE24DUXOA")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")


json_schema = StructType([StructField("TransactionID", StringType(), True),
                          StructField("Timestamp", StringType(), True),
                          StructField("UserID", StringType(), True),
                          StructField("TransactionType", StringType(), True),
                          StructField("TransactionAmount", StringType(), True),
                          StructField("Currency", StringType(), True),
                          StructField("PaymentMethod", StringType(), True),
                          StructField("PaymentStatus", StringType(), True),
                          StructField("MerchantID", StringType(), True),
                          StructField("MerchantName", StringType(), True),
                          StructField("TransactionDescription", StringType(), True),
                          StructField("TransactionSource", StringType(), True),
                          StructField("SessionID", StringType(), True),
                          StructField("UserIPAddress", StringType(), True),
                          StructField("UserAgent", StringType(), True),
                          StructField("DeviceType", StringType(), True),
                          StructField("DeviceOS", StringType(), True),
                          StructField("DeviceBrowser", StringType(), True),
                          StructField("DeviceScreenResolution", StringType(), True),
                          StructField("TimeZone", StringType(), True),
                          StructField("Location", StringType(), True),
                          StructField("PaymentGatewayID", StringType(), True),
                          StructField("PaymentProcessorID", StringType(), True),
                          StructField("AuthorizationCode", StringType(), True),
                          StructField("TransactionResponseCode", StringType(), True),
                          StructField("FraudScore", StringType(), True),
                          StructField("RiskFlag", StringType(), True),
                          StructField("AuthenticationMethod", StringType(), True),
                          StructField("Coupon", StringType(), True),
                          StructField("ShippingInformation", StringType(), True),
                          StructField("ReferringURL", StringType(), True),
                          StructField("PromoCode", StringType(), True),
                          StructField("Timestamp", StringType(), True),                          
                         ])

# df=spark.read.option('multiline','true').json('s3a://datalake-store-poc/raw/stream/message_0.json')
# # df=spark.read.option('multiline','true').json('sample_data1.json')
# json_array=df.toJSON().collect()
# json_str=json.dumps(json_array)
# print(json_array)

df=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","streamingData_api").load()
# json_df = df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json("value", json_schema).alias("data")) \
#     .select("data.*")
output_directory="s3a://datalake-store-poc/processed"
# output_directory="json_files"
# json_df.printSchema()

# query = json_df.writeStream.outputMode("append").format("json").option("path",output_directory).start()


query = df.writeStream.outputMode("append").format("json").option("path",output_directory).start()



# Perform any transformations as needed
# For example, you can add a timestamp column and select relevant fields
# transformed_df = streaming_df.withColumn("timestamp", current_timestamp()).select("field1", "field2", "timestamp")
# streaming_query = (
#     transformed_df.writeStream
#     .format("json")
#     .outputMode("append")  # Use "append" or "overwrite" based on your use case
#     .option("path", s3_output_path)
#     .start()
# )

query.awaitTermination()

spark.stop()