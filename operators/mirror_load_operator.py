from datetime import datetime

import pendulum
import snowflake.connector
from airflow.models import BaseOperator
import subprocess
from airflow.utils.decorators import apply_defaults

class MirrorLoadOperator(BaseOperator):
    def __init__(self, s3_conn_id,snowflake_conn_id,bucket_name,s3_configs_path, dataset_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.s3_configs_path = s3_configs_path
        self.dataset_name = dataset_name
        self.s3_conn_id = s3_conn_id
        self.snowflake_conn_id = snowflake_conn_id


    def execute(self, context):
        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(),pendulum.tz.UTC).strftime('%Y-%m-%d')

        try:
            # Build the command
            dbt_build_str = f""" dbt run --select tag:{self.dataset_name} --vars "{{'run_date': '{dag_run_date}'}}" """

            command = ['/usr/bin/bash','-c', f' python /opt/dbt_snowflake/generate_models.py --bucket_name "{self.bucket_name}" --configs_path  "{self.s3_configs_path}" \
                         --run_date "{dag_run_date}" --mode "airflow" --force_download "true" \
                        --s3_conn_id "{self.s3_conn_id}" --snowflake_conn_id "{self.snowflake_conn_id}" \
                        --dataset_name "{self.dataset_name}" --dbt_command "{dbt_build_str}" ' ]


            # Log the command being executed
            self.log.info(f"Executing command: {command}")

            # Run the command
            result = subprocess.run(command,shell=True, capture_output=True, text=True, check=True)

            # Log the output
            if  len(result.stdout)>0:
                self.log.info(f"Command output: {result.stdout}")
            else:
                self.log.error(f"Command output: {result.stderr}")
                raise Exception(f"Command output: {result.stderr}")

            return result.stdout
        except subprocess.CalledProcessError as e:
            self.log.error(f"Command failed with error: {e.stderr}")
            raise
