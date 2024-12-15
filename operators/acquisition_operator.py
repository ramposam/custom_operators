import re

import boto3
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class AcquisitionOperator(BaseOperator):
    def __init__(self,s3_conn_id, bucket_name,dataset_dir, file_pattern, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.file_pattern = file_pattern
        self.dataset_dir = dataset_dir
        self.s3_client = S3Hook(aws_conn_id=s3_conn_id).get_conn()

    def execute(self, context):

        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.dataset_dir)

        # Check if any contents are returned
        if 'Contents' not in response:
            print(f"No files found under prefix '{self.dataset_dir}' in bucket '{self.bucket_name}'.")
            return []

        # Compile the regex pattern
        pattern = re.compile(self.file_pattern)

        # Filter keys based on the regex pattern
        matching_files = [obj['Key'] for obj in response['Contents'] if pattern.search(obj['Key'])]

        if matching_files:

            context['ti'].xcom_push(key="files_found",value=matching_files)
            print(f"Found matching files: {matching_files}")
        else:
            print(f"No files matching pattern '{self.file_pattern}' found under prefix '{self.dataset_dir}'.")

        return matching_files