import csv
import json
import os
import boto3
from datetime import datetime

def write_csv_files(json_data, output_directory):
    data = json.loads(json_data)

    csv_writers = {}

    customers=data['customers']['customer']

    for customer in customers:
        creation_date_str = customer['creationDate']
        creation_date = datetime.strptime(creation_date_str, '%Y-%m-%d %H:%M')
        
        month_year = creation_date.strftime('%Y-%m')

        if month_year not in csv_writers:
            filename = f"{output_directory}/{month_year}.csv"
            with open(filename, mode='w', newline='') as csv_file:
                fieldnames = customer.keys()
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(customer)
                csv_writers[month_year] = writer
            csv_file.close()

        else:
            filename = f"{output_directory}/{month_year}.csv"
            with open(filename, mode='a', newline='') as csv_file:
                fieldnames = customer.keys()
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writerow(customer)
            csv_file.close()


def upload_csv_files_to_s3(directory_path, bucket_name, s3_prefix='datewise_csv'):

    session = boto3.Session(
    aws_access_key_id='AKIAZ7A2D7Q6BQDZDGYR',
    aws_secret_access_key='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')
    s3 = session.client('s3')

    files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]

    for file_name in files:
        local_file_path = os.path.join(directory_path, file_name)
        s3_key = os.path.join(s3_prefix, file_name) if s3_prefix else file_name

        try:
            s3.upload_file(local_file_path, bucket_name, s3_key)
            s3_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
            print(f"Uploaded {local_file_path} to {s3_url}")
        except Exception as e:
            print(f"Error uploading {local_file_path} to S3: {str(e)}")


if __name__ == "__main__":
    local_directory = "csv_files"
    aws_bucket_name = "kafka-s3-connection-bucket"
    s3_object_prefix = "sample_csv"

    uploaded_urls = upload_csv_files_to_s3(local_directory, aws_bucket_name, s3_object_prefix)
