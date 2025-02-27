import os
from datetime import datetime

import pendulum
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
import subprocess


class PostgresLoadToStageOperator(BaseOperator):
    def __init__(self, s3_conn_id,db_conn_id,bucket_name,s3_configs_path, dataset_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.s3_configs_path = s3_configs_path
        self.dataset_name = dataset_name
        self.s3_conn_id = s3_conn_id
        self.db_conn_id = db_conn_id

    def execute_dbt_command(self, dbt_command):
        """
        Execute the dbt command as a subprocess.
        """
        try:
            self.log.info(f"Running dbt command: {dbt_command}")
            process = subprocess.Popen(
                dbt_command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            stdout, stderr = process.communicate()

            if len(stdout) > 0:
                self.log.info(f"Command output: {stdout}")
            else:
                self.log.error(f"Command output: {stderr}")
                raise Exception(f"Command output: {stderr}")

            return stdout
        except subprocess.CalledProcessError as e:
            self.log.error(f"Command failed with error: {e.stderr}")
            raise

    def set_postgres_env_vars(self):
        # Extract details
        postgres_conn = BaseHook.get_connection(self.db_conn_id)

        # Extract connection details
        postgres_user = postgres_conn.login  # Username
        postgres_password = postgres_conn.password  # Password
        postgres_host = postgres_conn.host  # Account name (e.g., "account.region.postgrescomputing.com")
        postgres_database = postgres_conn.schema
        postgres_extra = postgres_conn.extra_dejson  # Parse JSON in "Extra" field
        self.log.info(f"postgres :{postgres_host}.")
        # Set environment variables
        os.environ["POSTGRES_USER"] = postgres_user
        os.environ["POSTGRES_PASSWORD"] = postgres_password
        os.environ["POSTGRES_HOST"] = postgres_host
        os.environ["POSTGRES_DATABASE"] = postgres_database

        self.log.info("postgres environment variables set successfully.")

    def execute(self, context):
        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(),pendulum.tz.UTC).strftime('%Y-%m-%d')

        self.set_postgres_env_vars()

        # Build the command
        dbt_build_str = f" cd /opt/airflow/dbt/ &&  dbt run --select tag:{self.dataset_name}-stage --vars \\\" {{\'run_date\': \'{dag_run_date}\'}} \\\" "

        command = f' python /opt/airflow/generate_models.py --bucket_name "{self.bucket_name}" --configs_path  "{self.s3_configs_path}" ' \
                  f' --run_date "{dag_run_date}" --mode "airflow" --force_download "true" ' \
                  f' --s3_conn_id "{self.s3_conn_id}" --db_conn_id "{self.db_conn_id}" ' \
                  f' --dataset_name "{self.dataset_name}" --dbt_command "{dbt_build_str}" '


        self.execute_dbt_command(command)

