from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,from_json
from pyspark.sql.types import StringType,StructField,StructType
import random
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
csv_output_path='fraud_csv/'

def assign_random_confidence():
    return str(random.randint(0, 100))
random_confidence_udf = udf(assign_random_confidence, StringType())

spark = SparkSession.builder.appName("StreamFraudDetection").getOrCreate()

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
                         ])

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "streamingData_api").load()
df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", json_schema).alias("data")) \
    .select("data.*")

df=df.withColumn("Confidence",random_confidence_udf())
df=df.withColumn("IsFraud",(df["Confidence"]>50).cast("string"))
df.printSchema()

query=df.writeStream.format('csv').outputMode('append').option('path',csv_output_path).option('checkpointLocation','checkpoint_files').start()
query.awaitTermination()
spark.stop()
