from pyspark.sql import SparkSession
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os

try:
    spark = SparkSession.builder.appName("CSV to Kafka").getOrCreate()

    csv_path = "output2.csv"
    try:
        if os.path.exists(csv_path):
            df = spark.read.csv(csv_path, header=True, inferSchema=True)
            if not df.head(1):
                raise Exception("The CSV file is empty")
        else:
            raise Exception("The path does not exists /"+csv_path)
        
        kafka_bootstrap_servers = "localhost:9092"
        kafka_topic = "sparkKafka"
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=lambda v: str(v).encode('utf-8')
            )

            print(df.collect())

            for row in df.collect():
                data = row.asDict()
                try:
                    producer.send(kafka_topic, value=data)
                except KafkaError as ke:
                    print("Error in sending the messages to kafka topic: "+ke)
                producer.flush()

            producer.close()

        except Exception as kafkaerror:
            print(kafkaerror)
        
            
    except Exception as e:
        print(e)

    spark.stop()

except Exception as sparkerror:
    print("Spark error: "+sparkerror)
