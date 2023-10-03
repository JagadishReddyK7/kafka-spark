from pyspark.sql import SparkSession
from generate_upload_csv import write_csv_files,upload_csv_files_to_s3
import json, os , datetime
from pyspark.sql.functions import col, from_json, udf, split
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType



spark = SparkSession.builder.config('spark.master','local').\
        config('spark.jars.packages','org.apache.hadoop:hadoop-aws:3.0.0,org.apache.hadoop:hadoop-common:3.0.0').\
        config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider').\
        getOrCreate()

data_schema = StructType([
    StructField("_id", StructType([StructField("$oid", StringType(), True)]), True),
    StructField("TransactionID", StringType(), True),
    StructField("Timestamp", StructType([StructField("$date", StringType(), True)]), True),
    StructField("UserID", StringType(), True),
    StructField("TransactionType", StringType(), True),
    StructField("TransactionAmount", DecimalType(10, 2), True),
    StructField("Currency", StringType(), True),
    StructField("PaymentMethod", StringType(), True),
    StructField("PaymentStatus", StringType(), True),
    StructField("MerchantID", IntegerType(), True),
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
    StructField("FraudScore", IntegerType(), True),
    StructField("RiskFlag", StringType(), True),
    StructField("AuthenticationMethod", StringType(), True),
    StructField("Coupon", StringType(), True),
    StructField("ShippingInformation", StringType(), True),
    StructField("ReferringURL", StringType(), True),
    StructField("PromoCode", IntegerType(), True)
])

sc=spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "")

df = spark.read.option('multiline','true').json('s3a://raw-transaction-data-faked/batch_ingestion/*', schema=data_schema)
df = df.withColumn("date", split(col("Timestamp.$date"), "T")[0])
df = df.drop("_id")
df = df.drop("Timestamp")
output_folder_path = "/home/vivek/jagadish/csv_files"
unique_dates = df.select("date").distinct().rdd.flatMap(lambda x: x).collect()
print(len(unique_dates))
for date in unique_dates:
        date_str =datetime.datetime.strptime(date,"%Y-%m-%d")
        date_folder_path = os.path.join(output_folder_path, str(date))
        filtered_df = df.filter(col("date") == date)
        csv_file_path =  f"{date_folder_path}.csv"
        #print(csv_file_path)
        filtered_df.toPandas().to_csv(csv_file_path)
        #filtered_df.write.csv(csv_file_path, header=True, mode="overwrite")
        local_directory='csv_files'
        bucket_name = 's3-processed-data-for-snowflake'
        upload_csv_files_to_s3(local_directory,bucket_name,'data-wise-csv/')

spark.stop()
