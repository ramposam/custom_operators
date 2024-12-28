import re

import boto3
import pendulum
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

class AcquisitionOperator(BaseOperator):
    def __init__(self,s3_conn_id, bucket_name,dataset_dir, file_pattern,datetime_pattern, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.datetime_pattern = datetime_pattern
        self.file_pattern = file_pattern
        self.dataset_dir = dataset_dir
        self.s3_client = S3Hook(aws_conn_id=s3_conn_id).get_conn()

    def execute(self, context):

        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.dataset_dir)

        # Check if any contents are returned
        if 'Contents' not in response:
            self.log.info(f"No files found under prefix '{self.dataset_dir}' in bucket '{self.bucket_name}'.")
            return []

        self.log.info(f"""data_interval_end:{context["data_interval_end"]}""")

        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(),pendulum.tz.UTC).strftime(self.datetime_pattern)
        file_pattern = self.file_pattern.format(datetime_pattern=dag_run_date) if "datetime_pattern" in self.file_pattern else self.file_pattern

        self.log.info(f"file_pattern:{file_pattern}")

        # Compile the regex pattern
        pattern = re.compile(file_pattern)

        # Filter keys based on the regex pattern
        matching_files = [obj['Key'] for obj in response['Contents'] if pattern.search(obj['Key'])]

        if matching_files:
            context['ti'].xcom_push(key="files_found",value=matching_files)
            self.log.info(f"Found matching files: {matching_files}")
        else:
            self.log.error(f"No files matching pattern '{file_pattern}' found under prefix '{self.dataset_dir}'.")
            raise Exception(f"No files matching pattern '{file_pattern}' found under prefix '{self.dataset_dir}'.")

        return matching_files