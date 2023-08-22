from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, to_json
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("WriteCsvToKafka").getOrCreate()

csv_file_path = "Mall_Customers.csv"
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
value_struct = struct(*[df[col_name].cast(StringType()).alias(col_name) for col_name in df.columns])
df_with_value_struct = df.selectExpr("CAST(null AS STRING) AS key", "value_struct AS value")
query = df_with_value_struct.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "sparkToKafka") \
    .start()
query.awaitTermination()
spark.stop()

