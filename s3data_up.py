import boto3

session = boto3.Session(
    aws_access_key_id='AKIAZ7A2D7Q6BQDZDGYR',
    aws_secret_access_key='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'
)
s3 = session.client('s3')
local_file_path = '/home/kafka/jagadish/Mall_Customers.csv'
bucket_name = 'kafka-s3-connection-bucket'
s3_object_key = 'zipcodes.json'
s3.upload_file(local_file_path, bucket_name, s3_object_key)
print("File uploaded to S3 successfully.")
