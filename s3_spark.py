from pyspark.sql import SparkSession
from generate_upload_csv import write_csv_files,upload_csv_files_to_s3
import json

spark = SparkSession.builder.config('spark.master','local').\
        config('spark.jars.packages','org.apache.hadoop:hadoop-aws:3.0.0,org.apache.hadoop:hadoop-common:3.0.0').\
        config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider').\
        getOrCreate()

sc=spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA3JCX6CPUE24DUXOA")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

# df=spark.read.option('multiline','true').json('s3a://datalake-store-poc/raw/stream/message_0.json')
df=spark.read.option('multiline','true').json('sample_data1.json')
json_array=df.toJSON().collect()
json_str=json.dumps(json_array)
print(json_array)
write_csv_files(json_str,'csv_files')

local_directory='csv_files'
bucket_name = 'datalake-store-poc'
upload_csv_files_to_s3(local_directory,bucket_name,'data-wise_csv')

df.printSchema()
df.show()

spark.stop()