import os
import tempfile
from datetime import datetime
from pathlib import Path

import pendulum
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from core_utils import s3_utils
from core_utils.config_reader_dbt import ConfigReaderDBT
import pandas as pd

class FilePostgresTableSchemaCheckOperator(BaseOperator):
    def __init__(self, db_conn_id, s3_conn_id=None, bucket_name=None, configs_path=None, dataset_name=None, encoding=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_name = dataset_name
        self.bucket_name = bucket_name
        self.s3_conn_id = s3_conn_id
        self.configs_path = configs_path
        self.encoding = encoding
        self.postgres_conn = PostgresHook(postgres_conn_id=db_conn_id).get_conn()

    def get_file_details(self,run_date):
        temp_dir = tempfile.mkdtemp()
        local_dir = os.path.join(temp_dir, "configs", self.dataset_name)

        Path(local_dir).mkdir(exist_ok=True,parents=True)
        self.log.info(f"Temporary directory created:{local_dir} to load configs" )

        if self.bucket_name and self.bucket_name != "None":
            # Use S3 to download configs
            s3_folder = f"{os.path.join(self.configs_path, self.dataset_name)}"  # "dataset_configs/dev"
            s3_utils.download_s3_folder(self.s3_conn_id, self.bucket_name, s3_folder, local_dir)
            self.log.info(f"Configs downloaded from S3 to {local_dir}")
        else:
            # Use local config path
            source_dir = os.path.join(self.configs_path, self.dataset_name)
            if os.path.exists(source_dir):
                import shutil
                shutil.copytree(source_dir, local_dir, dirs_exist_ok=True)
                self.log.info(f"Configs copied from local path {source_dir} to {local_dir}")
            else:
                raise Exception(f"Local config path {source_dir} does not exist")

        reader = ConfigReaderDBT(dataset_configs_path=local_dir,
                                 dataset_name=self.dataset_name,
                                 run_date=run_date)
        configs = reader.get_configs()
        self.log.info(f"Configs Read:{configs} ")
        return local_dir,configs

    # Function to clean column names
    def clean_column_names(self,columns):
        return [col.replace(" ", "_").upper() for col in columns]

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
        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(),pendulum.tz.UTC).strftime('%Y-%m-%d')
        configs_downloaded_tmp_dir,configs = self.get_file_details(dag_run_date)

        all_upstream_task_ids = self.get_all_upstream_task_ids(context)
        rawfile_path = context['ti'].xcom_pull(task_ids=all_upstream_task_ids[0],key='downloaded_file_path')
        file_format_params = configs[self.dataset_name]["mirror"]["file_format_params"]
        delimiter = file_format_params["delimiter"]

        self.log.info(f"Reading file from temporary area: {rawfile_path}")
        # Read file as DataFrame
        df = pd.read_csv(rawfile_path, delimiter=delimiter, encoding=self.encoding,nrows=100)

        # Transform column names
        df.columns = self.clean_column_names(df.columns)
        file_cols = list(df.columns)

        self.log.info(f"Received file header columns: {file_cols}")
        file_cols_cnt = len(file_cols)

        file_config_columns = list(configs[self.dataset_name]["mirror"]["file_schema"].keys())
        self.log.info(f"Configured columns : {file_config_columns}")


        file_config_cols_str = ",".join(file_config_columns)
        file_cols_str = ",".join(file_cols)

        if not set(file_cols) == set(file_config_columns):
            raise(Exception(f"Received file columns:{file_cols_str} and Configured file cols:{file_config_cols_str} are not equal. "))
        else:
            self.log.info(f"Received file columns:{file_cols_str} and Configured file cols:{file_config_cols_str} are  equal. ")
