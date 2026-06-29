import os
from datetime import datetime
from airflow.hooks.base import BaseHook

import pendulum
from airflow.models import BaseOperator
import subprocess


class PostgresMirrorTestsOperator(BaseOperator):
    def __init__(self, s3_conn_id=None, db_conn_id=None, bucket_name=None, configs_path=None, dataset_name=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.configs_path = configs_path
        self.dataset_name = dataset_name
        self.s3_conn_id = s3_conn_id
        self.db_conn_id = db_conn_id

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

    def execute_dbt_command(self, dbt_command):
        """
        Execute the dbt command as a subprocess.
        """
        self.set_postgres_env_vars()

        try:
            self.log.info(f"Running dbt command: {dbt_command}")
            process = subprocess.Popen(
                dbt_command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )

            stdout, stderr = process.communicate()

            if process.returncode == 0:
                self.log.info(f"Command output: {stdout}")
            else:
                self.log.error(f"Command output: {stdout}")
                raise Exception(f"Command output: {stdout}")

            return stdout
        except subprocess.CalledProcessError as e:
            self.log.error(f"Command failed with error: {e.stderr}")
            raise

    def execute(self, context):
        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(),pendulum.tz.UTC).strftime('%Y-%m-%d')

        # Build the command
        dbt_build_str = f" cd /opt/airflow/dbt/ &&  dbt test --select tag:{self.dataset_name}-mirror --vars \\\" {{\'run_date\': \'{dag_run_date}\'}} \\\" "

        if self.bucket_name and self.bucket_name != "None":
            # Use S3
            command = f' python /opt/airflow/generate_models.py --bucket_name "{self.bucket_name}" --configs_path  "{self.configs_path}" ' \
                      f' --run_date "{dag_run_date}" --mode "airflow" --force_download "true" ' \
                      f' --s3_conn_id "{self.s3_conn_id}" --db_conn_id "{self.db_conn_id}" ' \
                      f' --dataset_name "{self.dataset_name}" --dbt_command "{dbt_build_str}"   --layer "mirror"    --db_type "POSTGRES"  '
        else:
            # Use local file system
            command = f' python /opt/airflow/generate_models.py --configs_path "{self.configs_path}" ' \
                      f' --run_date "{dag_run_date}" --mode "airflow" ' \
                      f' --db_conn_id "{self.db_conn_id}" ' \
                      f' --dataset_name "{self.dataset_name}" --dbt_command "{dbt_build_str}"   --layer "mirror"    --db_type "POSTGRES"  '

        self.execute_dbt_command(command)

