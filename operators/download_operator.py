import os
import tempfile

import boto3
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class DownloadOperator(BaseOperator):

    def __init__(self, s3_conn_id, bucket_name,dataset_dir,file_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.file_name = file_name
        self.dataset_dir = dataset_dir
        self.s3_client = S3Hook(aws_conn_id=s3_conn_id).get_conn()

    def execute(self, context):

        temp_dir = tempfile.mkdtemp()
        print(f"Temporary directory created at: {temp_dir}")

        try:
            files_found = context['ti'].xcom_pull(key="files_found")
            # Download the file
            files_downloaded = []
            for file_name in files_found:
                download_path = os.path.join(temp_dir, os.path.basename(self.file_name))
                self.log.info(
                    f"Downloading file '{file_name}' from bucket '{self.bucket_name}' to '{download_path}'.")

                self.s3_client.download_file(self.bucket_name, file_name, download_path)
                files_downloaded.append(download_path)
                self.log.info(f"File '{file_name}' successfully downloaded to '{download_path}'.")

            context['ti'].xcom_push(key='downloaded_file_path',value=download_path)
            context['ti'].xcom_push(key='files', value=files_downloaded)

            return download_path
        except Exception as ex:
            self.log.error(ex.__str__())
            return None