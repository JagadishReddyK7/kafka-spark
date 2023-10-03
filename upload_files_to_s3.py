import boto3
import os
import time
from generate_upload_csv import file_already_uploaded,mark_file_as_uploaded

def upload(directory_path,s3_prefix,file_type):
    session = boto3.Session(
    aws_access_key_id='',
    aws_secret_access_key='')
    s3 = session.client('s3')

    bucket_name='s3-processed-data-for-snowflake'

    while True:

        files = [f for f in os.listdir(directory_path) if f.endswith(file_type)]

        for file_name in files:
            if not file_already_uploaded(file_name):
                local_file_path = os.path.join(directory_path, file_name)
                s3_key = os.path.join(s3_prefix, file_name) if s3_prefix else file_name

                try:
                    s3.upload_file(local_file_path, bucket_name, s3_key)
                    s3_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
                    mark_file_as_uploaded(file_name)
                    print(f"Uploaded {local_file_path} to {s3_url}")
                except Exception as e:
                    print(f"Error uploading {local_file_path} to S3: {str(e)}")
        
        time.sleep(5)

if __name__=='__main__':
    file_type=input("Enter the type of files to be uploaded: ")
    if file_type=='json':
        json_directory='json_files'
        upload(json_directory,'stream/','.json')
    elif file_type=='csv':
        csv_directory='fraud_csv'
        upload(csv_directory,'stream/','.csv')
