import boto3

session = boto3.Session(
    aws_access_key_id='',
    aws_secret_access_key=''
)
s3 = session.client('s3')
local_file_path = '/home/kafka/jagadish/sample_data1.json'
bucket_name = 'datalake-store-poc'
s3_object_key = 'sample_data.json'
s3.upload_file(local_file_path, bucket_name, s3_object_key)
print("File uploaded to S3 successfully.")
