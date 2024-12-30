import os
from datetime import datetime
from airflow.hooks.base import BaseHook

import pendulum
from airflow.models import BaseOperator
import subprocess

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class MirrorLoadOperator(BaseOperator):
    def __init__(self, s3_conn_id,snowflake_conn_id,bucket_name,s3_configs_path, dataset_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.s3_configs_path = s3_configs_path
        self.dataset_name = dataset_name
        self.s3_conn_id = s3_conn_id
        self.snowflake_conn_id = snowflake_conn_id

    def set_snowflake_env_vars(self):
        # Extract details
        sf_conn = BaseHook.get_connection(self.snowflake_conn_id)

        # Extract connection details
        snowflake_user = sf_conn.login  # Username
        snowflake_password = sf_conn.password  # Password
        snowflake_account = sf_conn.host  # Account name (e.g., "account.region.snowflakecomputing.com")
        snowflake_extra = sf_conn.extra_dejson  # Parse JSON in "Extra" field
        self.log.info(f"Snowflake :{snowflake_account}.")
        # Set environment variables
        os.environ["SNOWFLAKE_USER"] = snowflake_user
        os.environ["SNOWFLAKE_PASSWORD"] = snowflake_password
        os.environ["SNOWFLAKE_ACCOUNT"] = "mwpwekh-kc41785"
        os.environ["SNOWFLAKE_DATABASE"] = "MIRROR_DB"
        os.environ["SNOWFLAKE_SCHEMA"] = "MIRROR"

        # Optionally, set other variables from the `extra` field
        if "warehouse" in snowflake_extra:
            os.environ["SNOWFLAKE_WAREHOUSE"] = snowflake_extra["warehouse"]
        if "role" in snowflake_extra:
            os.environ["SNOWFLAKE_ROLE"] = snowflake_extra["role"]

        self.log.info("Snowflake environment variables set successfully.")

    def execute_dbt_command(self, dbt_command):
        """
        Execute the dbt command as a subprocess.
        """
        self.set_snowflake_env_vars()

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

    def execute(self, context):
        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(),pendulum.tz.UTC).strftime('%Y-%m-%d')

        # Build the command
        dbt_build_str = f" cd /opt/airflow/dbt/ &&  dbt run --select tag:{self.dataset_name}-mirror --vars \\\" {{\'run_date\': \'{dag_run_date}\'}} \\\" "

        command = f' python /opt/airflow/generate_models.py --bucket_name "{self.bucket_name}" --configs_path  "{self.s3_configs_path}" ' \
                  f' --run_date "{dag_run_date}" --mode "airflow" --force_download "true" ' \
                  f' --s3_conn_id "{self.s3_conn_id}" --snowflake_conn_id "{self.snowflake_conn_id}" ' \
                  f' --dataset_name "{self.dataset_name}" --dbt_command "{dbt_build_str}" '


        self.execute_dbt_command(command)

