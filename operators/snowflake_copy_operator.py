import os.path

import snowflake.connector
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from helper_utils.file_utils import read_and_infer,identify_delimiter

class SnowflakeCopyOperator(BaseOperator):
    def __init__(self, snowflake_conn_id, stage_name,table_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stage_name = stage_name
        self.table_name = table_name
        self.sf_conn = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_conn()

    def get_snowflake_stg_file_details(self):
        list_files_query = f"list @{self.stage_name}"
        with self.sf_conn.cursor() as cur:
            cur.execute(list_files_query)
            result = cur.fetchall()
            stage_file_name = result[0][0].split("/")[-1]
            file_name = stage_file_name.replace(".gz", "") if stage_file_name.endswith(".gz") else stage_file_name
            db_schema = ".".join(self.table_name.split(".")[:-1])
            file_format = f"{db_schema}.ff_{file_name.lower().split('.')[0]}"
            compression = "gzip" if stage_file_name != "csv" else "NONE"
            self.log.info(f"Stage file:{stage_file_name}, file_format:{file_format},file_compression_type:{compression}")
        return stage_file_name,file_format,compression

    def create_file_format(self,conn, file_format_name, file_type="CSV", delimiter=",", compression="NONE"):
        # Define the SQL command to create the file format

        create_file_format_sql = f"""
        CREATE OR REPLACE FILE FORMAT {file_format_name}
        TYPE = {file_type}
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        FIELD_DELIMITER = '{delimiter}'
        SKIP_HEADER = 1        
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

        cols_list = ",".join([f"${index+1} as {col_name.upper()}" for index, col_name in enumerate(columns)])
        # Define the SQL command to load data from the stage into the table
        copy_sql = f"""        
        COPY INTO "DEVOPS_DB"."DEVOPS"."T_NETFLIX_DATA"
        FROM (
            SELECT {cols_list}
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

        stage_file_name,file_format, file_compression = self.get_snowflake_stg_file_details()

        self.create_file_format(conn=self.sf_conn,delimiter=delimiter, file_format_name=file_format,compression=file_compression)
        self.copy_into_table(self.sf_conn, self.stage_name, self.table_name, columns, file_format, stage_file_name)