from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaStream").getOrCreate()

# Define the Kafka source parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "streamingData_api"

# Read streaming data from Kafka
df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .load()
)

# Start the streaming query
query = df.writeStream.outputMode("append").format("console").start()

# Wait for the query to terminate
query.awaitTermination()
