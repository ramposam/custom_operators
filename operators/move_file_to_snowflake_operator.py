import snowflake.connector
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class MoveFileToSnowflakeOperator(BaseOperator):
    def __init__(self, snowflake_conn_id, stage_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stage_name = stage_name
        self.sf_conn = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_conn()


    def execute(self, context):

        file_path = context['ti'].xcom_pull(key='downloaded_file_path')
        cursor = self.sf_conn.cursor()
        cursor.execute(f"PUT file://{file_path} @{self.stage_name}")
        self.log.info(f"File {file_path} loaded to stage {self.stage_name}.")