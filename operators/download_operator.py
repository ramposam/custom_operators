import os
import tempfile

import boto3
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pendulum

class DownloadOperator(BaseOperator):

    def __init__(self, s3_conn_id=None, bucket_name=None, dataset_dir=None, file_name=None, datetime_pattern=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.datetime_pattern = datetime_pattern
        self.file_pattern = file_name
        self.dataset_dir = dataset_dir
        self.s3_conn_id = s3_conn_id
        
        # Only initialize S3 client if bucket_name is provided and not None/empty string
        if bucket_name and bucket_name != "None":
            self.s3_client = S3Hook(aws_conn_id=s3_conn_id).get_conn()
        else:
            self.s3_client = None

    def get_all_upstream_task_ids(self, context):
        """Get all upstream task IDs recursively by traversing the DAG structure in BFS order."""
        dag = context['dag']
        current_task_id = context['task'].task_id
        
        # Get all tasks in the DAG
        all_tasks = {task.task_id: task for task in dag.tasks}
        
        # Find all upstream tasks recursively using BFS to maintain order
        upstream_task_ids = []
        visited = set()
        queue = [current_task_id]
        
        while queue:
            task_id = queue.pop(0)
            if task_id in visited:
                continue
            visited.add(task_id)
            
            task = all_tasks.get(task_id)
            if task:
                for upstream_task_id in task.upstream_task_ids:
                    if upstream_task_id not in visited and upstream_task_id not in upstream_task_ids:
                        upstream_task_ids.append(upstream_task_id)
                        queue.append(upstream_task_id)
        
        return upstream_task_ids

    def execute(self, context):

        temp_dir = tempfile.mkdtemp()
        self.log.info(f"Temporary directory created at: {temp_dir}")

        self.log.info(f"""data_interval_end:{context["data_interval_end"]}""")

        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(), pendulum.tz.UTC).strftime( self.datetime_pattern)
        # self.file_name = self.file_name.replace("datetime_pattern", dag_run_date)
        file_pattern = self.file_pattern.format(datetime_pattern=dag_run_date) if "datetime_pattern" in self.file_pattern else self.file_pattern

        self.log.info(f"file_name:{file_pattern}")

        all_upstream_task_ids = self.get_all_upstream_task_ids(context)
        files_found = context['ti'].xcom_pull(task_ids=all_upstream_task_ids[0],key="files_found")

        if not len(files_found) > 0:
            self.log.error(f"No files found: {files_found} to download.")
            raise Exception(f"No files found: {files_found} to download.")

        try:

            # Download or copy the file
            files_downloaded = []
            for file_name in files_found:
                download_path = os.path.join(temp_dir, os.path.basename(file_pattern))
                
                if self.s3_client is None:
                    # Use local file system - copy from dataset_dir
                    self.log.info(
                        f"Copying file '{file_name}' from local path to '{download_path}'.")
                    import shutil
                    shutil.copy2(file_name, download_path)
                    files_downloaded.append(download_path)
                    self.log.info(f"File '{file_name}' successfully copied to '{download_path}'.")
                else:
                    # Use S3 - download from bucket
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