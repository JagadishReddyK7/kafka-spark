from pyspark.sql import SparkSession

spark = SparkSession.builder.config('spark.master','local').\
        config('spark.jars.packages','org.apache.hadoop:hadoop-aws:3.0.0,org.apache.hadoop:hadoop-common:3.0.0').\
        config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider').\
        getOrCreate()
        

sc=spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZ7A2D7Q6BQDZDGYR")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

df=spark.read.format('csv').load('s3a://kafka-s3-connection-bucket/Mall_Customers.csv')

df.printSchema()
df.show()

spark.stop()