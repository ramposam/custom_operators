import os.path

import snowflake.connector
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from core_utils.file_utils import read_and_infer,identify_delimiter

from operators.constants import mirror_file_meta_cols, mirror_meta_cols


class SnowflakeCopyOperator(BaseOperator):
    def __init__(self, snowflake_conn_id, stage_name,table_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stage_name = stage_name
        self.table_name = table_name
        self.file_format_props = kwargs.get("file_format",None)
        self.sf_conn = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_conn()

    def get_snowflake_stg_file_details(self):
        list_files_query = f"list @{self.stage_name}"
        with self.sf_conn.cursor() as cur:
            cur.execute(list_files_query)
            result = cur.fetchall()
            stage_file_name = result[0][0].split("/")[-1]
            file_name = stage_file_name.replace(".gz", "") if stage_file_name.endswith(".gz") else stage_file_name
            db_schema = ".".join(self.table_name.split(".")[:-1])
            file_format = f"""{db_schema}.ff_{self.stage_name.split(".")[-1][4:].lower().split('.')[0]}"""
            compression = "gzip" if stage_file_name != "csv" else "NONE"
            self.log.info(f"Stage file:{stage_file_name}, file_format:{file_format},file_compression_type:{compression}")
        return stage_file_name,file_format,compression

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
        COMPRESSION = {compression};
        """
        self.log.info(f"File format sql: {create_file_format_sql}")

        # Execute the SQL command to create the file format
        with conn.cursor() as cur:
            cur.execute(create_file_format_sql)
            self.log.info(f"File format {file_format_name} created successfully.")

    def copy_into_table(self,conn, stage_name, table_name,columns, file_format_name, file_path):

        cols_list_str = ",".join([f"${index+1} as {col_name.upper()}" for index, col_name in enumerate(columns)])

        meta_cols = []
        for col in mirror_file_meta_cols:
            meta_cols.append(f"metadata${col} as {col}")

        meta_cols_list_str = ",".join(meta_cols)

        # Define the SQL command to load data from the stage into the table
        copy_sql = f"""        
        COPY INTO {table_name}
        FROM (
            SELECT {cols_list_str},{meta_cols_list_str},
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
            cur.execute(copy_sql)
            self.log.info(f"Data loaded into {table_name} from @{stage_name}/{file_path}.")


    def execute(self, context):

        file_path = context['ti'].xcom_pull(key='downloaded_file_path')
        delimiter, columns, data_types = read_and_infer(file_path)
        skip_header = 1

        stage_file_name,file_format, file_compression = self.get_snowflake_stg_file_details()

        if self.file_format_props:
            delimiter = self.file_format_props["delimiter"]
            skip_header = self.file_format_props["skip_header"]
            file_compression = "GZIP" if self.file_format_props["compressed"] else "NONE"

        self.create_file_format(conn=self.sf_conn,delimiter=delimiter,skip_header=skip_header, file_format_name=file_format,compression=file_compression)

        self.copy_into_table(self.sf_conn, self.stage_name, self.table_name, columns, file_format, stage_file_name)