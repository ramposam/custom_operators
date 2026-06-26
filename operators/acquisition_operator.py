import re
import os

import boto3
import pendulum
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

class AcquisitionOperator(BaseOperator):
    def __init__(self, s3_conn_id=None, bucket_name=None, dataset_dir=None, file_pattern=None, datetime_pattern=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.datetime_pattern = datetime_pattern
        self.file_pattern = file_pattern
        self.dataset_dir = dataset_dir
        self.s3_conn_id = s3_conn_id
        
        # Only initialize S3 client if bucket_name is provided and not None/empty string
        if bucket_name and bucket_name != "None":
            self.s3_client = S3Hook(aws_conn_id=s3_conn_id).get_conn()
        else:
            self.s3_client = None

    def execute(self, context):
        # Use local file system if bucket_name is None or "None"
        if self.s3_client is None:
            # Use local file system
            dataset_path = self.dataset_dir
            if not os.path.exists(dataset_path):
                self.log.info(f"Directory '{dataset_path}' does not exist.")
                return []
            
            # Get all files in the directory
            all_files = []
            for root, dirs, files in os.walk(dataset_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    all_files.append(full_path)
            
            if not all_files:
                self.log.info(f"No files found under directory '{dataset_path}'.")
                return []
        else:
            # Use S3
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.dataset_dir)

            # Check if any contents are returned
            if 'Contents' not in response:
                self.log.info(f"No files found under prefix '{self.dataset_dir}' in bucket '{self.bucket_name}'.")
                return []
            
            all_files = [obj['Key'] for obj in response['Contents']]
        self.log.info(f"All files found:{all_files}")
        self.log.info(f"""data_interval_end:{context["data_interval_end"]}""")

        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(),pendulum.tz.UTC).strftime(self.datetime_pattern)
        file_pattern = self.file_pattern.format(datetime_pattern=dag_run_date) if "datetime_pattern" in self.file_pattern else self.file_pattern

        self.log.info(f"file_pattern:{file_pattern}")

        # Compile the regex pattern
        pattern = re.compile(file_pattern)

        # Filter files based on the regex pattern
        matching_files = [file_path for file_path in all_files if pattern.search(file_path)]

        if matching_files:
            context['ti'].xcom_push(key="files_found",value=matching_files)
            self.log.info(f"Found matching files: {matching_files}")
        else:
            location = f"directory '{self.dataset_dir}'" if self.s3_client is None else f"prefix '{self.dataset_dir}' in bucket '{self.bucket_name}'"
            self.log.error(f"No files matching pattern '{file_pattern}' found under {location}.")
            raise Exception(f"No files matching pattern '{file_pattern}' found under {location}.")

        return matching_files