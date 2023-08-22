import boto3
from confluent_kafka import Consumer, KafkaError
import uuid

def generate_unique_id():
    return str(uuid.uuid4())

aws_access_key_id = 'AKIAZ7A2D7Q6BQDZDGYR'
aws_secret_access_key = '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'
s3_bucket_name = 'kafka-s3-connection-bucket'

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mall_customer',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['mallCustomers'])

s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

while True:
    msg = consumer.poll(1.0)
    print(msg)
    if msg is None:
        print("message is empty")
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    json_content = msg.value().decode('utf-8')
    file_path='customer_data/'+generate_unique_id()+'.json'

    s3.put_object(Body=json_content, Bucket=s3_bucket_name, Key=file_path)
    print("Data uploaded to S3 successfully")

consumer.close()
