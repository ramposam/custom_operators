import os.path
import tempfile
from datetime import datetime
from pathlib import Path

import pendulum
import snowflake.connector
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from core_utils import s3_utils
from core_utils.config_reader_dbt import ConfigReaderDBT
from core_utils.file_utils import read_and_infer,identify_delimiter

from operators.constants import mirror_file_meta_cols, mirror_meta_cols


class SnowflakeCopyOperator(BaseOperator):
    def __init__(self, snowflake_conn_id,s3_conn_id,bucket_name,s3_configs_path, stage_name,table_name,dataset_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stage_name = stage_name
        self.table_name = table_name
        self.dataset_name = dataset_name
        self.bucket_name = bucket_name
        self.s3_conn_id = s3_conn_id
        self.s3_configs_path = s3_configs_path
        self.file_format_props = kwargs.get("file_format",None)
        self.sf_conn = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_conn()

    def get_snowflake_stg_file_details(self):
        try:
            list_files_query = f"list @{self.stage_name}"
            with self.sf_conn.cursor() as cur:
                cur.execute(list_files_query)
                result = cur.fetchall()
                stage_file_name = result[0][0].split("/")[-1]
                # file_name = stage_file_name.replace(".gz", "") if stage_file_name.endswith(".gz") else stage_file_name
                # db_schema = ".".join(self.table_name.split(".")[:-1])
                # file_format = f"""{db_schema}.ff_{self.stage_name.split(".")[-1][4:].lower().split('.')[0]}"""
                # compression = "gzip" if stage_file_name != "csv" else "NONE"
                self.log.info(f"Stage file:{stage_file_name}")
            return stage_file_name
        except Exception as ex:
            self.log.error(f"Query Result:{result} and error is:{ex.__str__()}")

    def create_file_format(self,conn, file_format_name, file_type="CSV", delimiter=",",skip_header=1, compression="NONE"):
        # Define the SQL command to create the file format

        create_file_format_sql = f"""
        CREATE OR REPLACE FILE FORMAT {file_format_name}
        TYPE = {file_type}
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        FIELD_DELIMITER = '{delimiter}'
        SKIP_HEADER = {skip_header}      
        TRIM_SPACE=TRUE,
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT='YYYY-MM-DD',
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
        ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
        COMPRESSION = {compression};
        """
        self.log.info(f"File format sql: {create_file_format_sql}")

        # Execute the SQL command to create the file format
        with conn.cursor() as cur:
            cur.execute(create_file_format_sql)
            self.log.info(f"File format {file_format_name} created successfully.")

    def copy_into_table(self,conn, stage_name, table_name,columns, file_format_name, file_path,run_date):

        cols_list_str = ",".join([f"${index+1} as {col_name.upper()}" for index, col_name in enumerate(columns)])

        meta_cols = []
        for col in mirror_file_meta_cols:
            meta_cols.append(f"metadata${col} as {col}")

        meta_cols_list_str = ",".join(meta_cols)

        truncate_sql = f"""TRUNCATE TABLE {table_name};"""

        # Define the SQL command to load data from the stage into the table
        copy_sql = f"""         
        COPY INTO {table_name}
        FROM (
            SELECT {cols_list_str},'{run_date}',{meta_cols_list_str},
            current_timestamp as created_dts, current_user as created_by
            FROM '@{stage_name}'
        )
        FILES = ('{file_path}')
        FILE_FORMAT = (FORMAT_NAME={file_format_name})
        FORCE = FALSE
        ON_ERROR = CONTINUE
        PURGE = TRUE;
        """

        self.log.info(f"File format sql: {copy_sql}")

        # Execute the SQL command to copy the data
        with conn.cursor() as cur:
            cur.execute(truncate_sql)
            cur.execute(copy_sql)
            self.log.info(f"Data loaded into {table_name} from @{stage_name}/{file_path}.")

    def get_file_details(self, run_date):
        temp_dir = tempfile.mkdtemp()
        local_dir = os.path.join(temp_dir, "configs", self.dataset_name)

        Path(local_dir).mkdir(exist_ok=True, parents=True)
        self.log.info(f"Temporary directory created:{local_dir} to download configs from s3")

        # Example usage
        s3_folder = f"{os.path.join(self.s3_configs_path, self.dataset_name)}"  # "dataset_configs/dev"

        s3_utils.download_s3_folder(self.s3_conn_id, self.bucket_name, s3_folder, local_dir)
        reader = ConfigReaderDBT(dataset_configs_path=local_dir,
                                 dataset_name=self.dataset_name,
                                 run_date=run_date)
        configs = reader.get_configs()
        self.log.info(f"Configs Read:{configs} ")
        return local_dir, configs

    def execute(self, context):
        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(),pendulum.tz.UTC).strftime('%Y-%m-%d')

        configs_downloaded_tmp_dir,configs = self.get_file_details(dag_run_date)

        file_format_params = configs[self.dataset_name]["mirror"]["file_format_params"]

        file_format_name = f"MIRROR_DB.MIRROR.ff_{self.dataset_name}".upper()
        delimiter = file_format_params["delimiter"]
        compression = 'GZIP' if file_format_params["compressed"] else None

        self.create_file_format(conn=self.sf_conn, delimiter=delimiter,
                                skip_header=file_format_params["skip_header"],
                                file_format_name=file_format_name,
                                compression=compression)

        stage_file_name = self.get_snowflake_stg_file_details()

        columns = list(configs[self.dataset_name]["mirror"]["file_schema"].keys())

        self.log.info(f"File schema: {columns}")

        self.copy_into_table(self.sf_conn, self.stage_name, self.table_name, columns, file_format_name, stage_file_name,dag_run_date)