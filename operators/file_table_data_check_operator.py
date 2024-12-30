import os
import tempfile
from datetime import datetime
from pathlib import Path
import pandas as pd
import pendulum
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from core_utils import s3_utils
from core_utils.config_reader_dbt import ConfigReaderDBT

from operators.constants import table_schema_query, file_schema_query, file_cols_query

class FileTableDataCheckOperator(BaseOperator):
    def __init__(self, snowflake_conn_id,s3_conn_id,bucket_name,s3_configs_path,table_name,dataset_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.dataset_name = dataset_name
        self.bucket_name = bucket_name
        self.s3_conn_id = s3_conn_id
        self.s3_configs_path = s3_configs_path
        self.sf_conn = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_conn()

    def get_file_details(self,run_date):
        temp_dir = tempfile.mkdtemp()
        local_dir = os.path.join(temp_dir, "configs", self.dataset_name)

        Path(local_dir).mkdir(exist_ok=True,parents=True)
        self.log.info(f"Temporary directory created:{local_dir} to download configs from s3" )

        # Example usage
        s3_folder = f"{os.path.join(self.s3_configs_path, self.dataset_name)}"  # "dataset_configs/dev"

        s3_utils.download_s3_folder(self.s3_conn_id, self.bucket_name, s3_folder, local_dir)

        reader = ConfigReaderDBT(dataset_configs_path=local_dir,
                                 dataset_name=self.dataset_name,
                                 run_date=run_date)
        configs = reader.get_configs()
        self.log.info(f"Configs Read:{configs} ")
        return local_dir,configs

    def compare_file_table_data(self, run_date, file_path, delimiter=","):
        # Query the target table
        mirror_db = self.table_name.split(".")[0] if "." in self.table_name else "MIRROR_DB"
        mirror_schema = self.table_name.split(".")[1] if "." in self.table_name else "MIRROR"
        mirror_table = self.table_name.split(".")[2] if "." in self.table_name else self.table_name
        table_cols_query = table_schema_query.format(mirror_db=mirror_db,
                                                     mirror_schema=mirror_schema,
                                                     mirror_table=mirror_table)
        self.log.info(f"Table columns query:{table_cols_query}")

        # Load the file data into a DataFrame
        df_staging = pd.read_csv(file_path, sep=delimiter)
        df_staging.columns = df_staging.columns.str.upper().str.replace(" ", "_")
        self.log.info(f"df_staging cols {df_staging.columns} ,{df_staging}")

        cursor = self.sf_conn.cursor()

        cursor.execute(f"{table_cols_query}")
        result = cursor.fetchall()

        self.log.info(f"Table columns: {result[0]}")

        table_cols_str = result[0][0]

        query = f"""
            SELECT {table_cols_str} FROM {mirror_db}.{mirror_schema}.{mirror_table}
            where FILE_DATE = '{run_date}'
            """
        self.log.info(f"Table Data Query:{query}")

        df_target = pd.read_sql(query, self.sf_conn)
        self.log.info(f"df_target cols {df_target.columns}, {df_target}")

        # Compare DataFrames
        comparison = pd.concat([df_target, df_staging]).drop_duplicates(keep=False)

        self.log.info(f"Differences between file and table: {comparison}")

        # Find mismatched rows
        # mismatched_rows = df_target.merge(df_staging, indicator=True, how='outer').query('_merge != "both"')
        # self.log.info(f"Differences between file and table: {mismatched_rows}")

    def execute(self, context):
        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(),pendulum.tz.UTC).strftime('%Y-%m-%d')
        file_path = context['ti'].xcom_pull(key='downloaded_file_path_duplicate')
        configs_downloaded_tmp_dir,configs = self.get_file_details(dag_run_date)

        file_format_params = configs[self.dataset_name]["mirror"]["file_format_params"]
        delimiter = file_format_params["delimiter"]

        self.compare_file_table_data(dag_run_date,file_path,delimiter)


