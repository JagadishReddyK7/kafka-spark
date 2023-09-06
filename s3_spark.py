from pyspark.sql import SparkSession
from generate_upload_csv import write_csv_files,upload_csv_files_to_s3
import json

spark = SparkSession.builder.config('spark.master','local').\
        config('spark.jars.packages','org.apache.hadoop:hadoop-aws:3.0.0,org.apache.hadoop:hadoop-common:3.0.0').\
        config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider').\
        getOrCreate()
        

sc=spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZ7A2D7Q6BQDZDGYR")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

# df=spark.read.option('multiline','true').json('s3a://kafka-s3-connection-bucket/zipcodes.json')
df=spark.read.option('multiline','true').json('sample_data.json')
json_array=df.toJSON().collect()
json_str=json.dumps(json_array)
write_csv_files(json_str,'csv_files')

local_directory='csv_files'
bucket_name = 'kafka-s3-connection-bucket'
upload_csv_files_to_s3(local_directory,bucket_name,'sample_csv1')

df.printSchema()
df.show()

spark.stop()