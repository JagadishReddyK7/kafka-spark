import boto3
import json

s3 = boto3.client('s3',aws_access_key_id='',aws_secret_access_key='')
response=s3.get_object(Bucket='kafka-s3-connection-bucket',Key='random_transactions_data.csv')
object_content=response['Body'].read().decode('utf-8')


print(object_content)
