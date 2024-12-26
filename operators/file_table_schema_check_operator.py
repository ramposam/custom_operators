import os
import tempfile
from datetime import datetime
from pathlib import Path

import pendulum
import snowflake.connector
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from core_utils import s3_utils
from core_utils.config_reader_dbt import ConfigReaderDBT

from operators.constants import table_schema_query, file_schema_query, file_cols_query


class FileTableSchemaCheckOperator(BaseOperator):
    def __init__(self, snowflake_conn_id,s3_conn_id,bucket_name,s3_configs_path, stage_name,table_name,dataset_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stage_name = stage_name
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


    def execute(self, context):
        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(),pendulum.tz.UTC).strftime('%Y-%m-%d')
        configs_downloaded_tmp_dir,configs = self.get_file_details(dag_run_date)

        file_format_params = configs[self.dataset_name]["mirror"]["file_format_params"]
        file_format_name = f"MIRROR_DB.MIRROR.ff_{self.dataset_name}_tmp".upper()
        delimiter = file_format_params["delimiter"]
        compression = 'GZIP' if file_format_params["compressed"] else None
        self.create_file_format(conn=self.sf_conn, delimiter=delimiter,
                                skip_header=int(file_format_params["skip_header"])-1,
                                file_format_name=file_format_name,
                                compression=compression)


        cursor = self.sf_conn.cursor()
        file_schema_query_formatted = file_schema_query.format(file_format_name=file_format_name,
                                 stage_name=self.stage_name)
        cursor.execute(f"{file_schema_query_formatted}")
        result = cursor.fetchall()
        self.log.info(f"File header columns count: {result[0]}")
        file_cols_cnt = result[0][0]
        query_select_cols_str  = ",".join([f"${index+1}" for index,val in enumerate(range(file_cols_cnt)) ])

        file_cols_query_formatted = file_cols_query.format(file_format_name=file_format_name,
                                 stage_name=self.stage_name,
                                 query_select_cols_str=query_select_cols_str)
        self.log.info(f"Stage file columns query:{file_cols_query_formatted}")
        cursor.execute(f"{file_cols_query_formatted}")
        result = cursor.fetchall()
        self.log.info(f"File header columns: {result[0]}")

        file_cols_ordered = [f"""{col.replace(" ","_").upper()}""" for col in list(result[0])]
        self.log.info(f"File columns formatted: {file_cols_ordered}")
        file_cols_str = ",".join(file_cols_ordered)

        mirror_db = self.table_name.split(".")[0] if "." in self.table_name else "MIRROR_DB"
        mirror_schema = self.table_name.split(".")[1] if "." in self.table_name else "MIRROR"
        mirror_table = self.table_name.split(".")[2] if "." in self.table_name else self.table_name

        table_cols_query = table_schema_query.format(mirror_db=mirror_db,
                                                     mirror_schema=mirror_schema,
                                                     mirror_table=mirror_table)
        self.log.info(f"Table columns query:{table_cols_query}")

        cursor.execute(f"{table_cols_query}")
        result = cursor.fetchall()

        self.log.info(f"Table columns: {result[0]}")

        table_cols_str = result[0][0]
        table_cols = table_cols_str.split(",")

        if not set(file_cols_ordered) == set(table_cols):
            raise(Exception(f"File columns:{file_cols_str} and Table cols:{table_cols_str} are not equal. "))
        else:
            self.log.info(f"File columns:{file_cols_str} and Table cols:{table_cols_str} are  equal. ")
