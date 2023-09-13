import csv
import json
import os
import boto3
from datetime import datetime
import time

def write_csv_files(json_data, output_directory):
    data = json.loads(json_data)
    print(data)

    csv_writers = {}

    for record in data:
        transaction=json.loads(record)
        creation_date_str = transaction['Timestamp']
        creation_date = datetime.strptime(creation_date_str, '%Y-%m-%d %H:%M:%S')
        
        month_year = creation_date.strftime('%Y-%m')

        if month_year not in csv_writers:
            filename = f"{output_directory}/{month_year}.csv"
            with open(filename, mode='w', newline='') as csv_file:
                fieldnames = transaction.keys()
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(transaction)
                csv_writers[month_year] = writer
            csv_file.close()

        else:
            filename = f"{output_directory}/{month_year}.csv"
            with open(filename, mode='a', newline='') as csv_file:
                fieldnames = transaction.keys()
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writerow(transaction)
            csv_file.close()


def upload_csv_files_to_s3(directory_path, s3_prefix):

    session = boto3.Session(
    aws_access_key_id='AKIA3JCX6CPUE24DUXOA',
    aws_secret_access_key='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')
    s3 = session.client('s3')
    bucket_name=''

    files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]

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



def file_already_uploaded(filename):
    with open('file_logs/file_logs.json','r') as json_file:
        data=json.load(json_file)
        if filename in data['file_list']:
            return True

def mark_file_as_uploaded(filename):
    with open('file_logs/file_logs.json','r') as json_file:
        data=json.load(json_file)
        data['file_list'].append(filename)
        json_object=json.dumps(data)
    with open('file_logs/file_logs.json','w') as json_file:
        json_file.write(json_object)


if __name__ == "__main__":
    local_directory = "csv_files"
    aws_bucket_name = "datalake-stor-poc"
    s3_object_prefix = "sample_csv"

    uploaded_urls = upload_csv_files_to_s3(local_directory, aws_bucket_name, s3_object_prefix)
