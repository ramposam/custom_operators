import os
import tempfile
from datetime import datetime
from pathlib import Path
import pandas as pd
import pendulum
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from core_utils import s3_utils
from core_utils.config_reader_dbt import ConfigReaderDBT

from operators.constants import  file_schema_query, file_cols_query, postgres_table_schema_query


class FilePostgresTableDataCheckOperator(BaseOperator):
    def __init__(self, db_conn_id,s3_conn_id,bucket_name,s3_configs_path,table_name,dataset_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.dataset_name = dataset_name
        self.bucket_name = bucket_name
        self.s3_conn_id = s3_conn_id
        self.s3_configs_path = s3_configs_path
        self.postgres_conn = PostgresHook(postgres_conn_id=db_conn_id).get_conn()

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

    def compare_dataframes(self,df1, df2):
        """
        Compare two DataFrames and return differences.
        """

        self.log.info(f"file: {df1.shape}, table: {df2.shape}")

        if df1.shape != df2.shape:
            self.log.info("DataFrames have different shapes. Comparison may be inconsistent.")

        # Compare DataFrames using pandas built-in method
        diff = df1.compare(df2, keep_equal=False)
        diff = diff.rename(columns={'self': 'file', 'other': 'table'})

        # Identify rows that are in df1 but not in df2
        df1_not_in_df2 = df1[~df1.apply(tuple, axis=1).isin(df2.apply(tuple, axis=1))]

        # Identify rows that are in df2 but not in df1
        df2_not_in_df1 = df2[~df2.apply(tuple, axis=1).isin(df1.apply(tuple, axis=1))]

        return {
            "value_differences": diff,  # Shows exact cell differences
            "rows_in_df1_not_in_df2": df1_not_in_df2,  # Rows unique to df1
            "rows_in_df2_not_in_df1": df2_not_in_df1  # Rows unique to df2
        }

    def compare_file_table_data(self, run_date, file_path, delimiter=","):
        # Query the target table
        mirror_db = self.table_name.split(".")[0] if "." in self.table_name else "MIRROR_DB"
        mirror_schema = self.table_name.split(".")[1] if "." in self.table_name else "MIRROR"
        mirror_table = self.table_name.split(".")[2] if "." in self.table_name else self.table_name
        table_cols_query = postgres_table_schema_query.format(mirror_db=mirror_db,
                                                     mirror_schema=mirror_schema,
                                                     mirror_table=mirror_table)
        self.log.info(f"Table columns query:{table_cols_query}")

        # Load the file data into a DataFrame
        file_df = pd.read_csv(file_path, sep=delimiter)
        file_df.columns = file_df.columns.str.upper().str.replace(" ", "_")
        self.log.info(f"file_df cols {list(file_df.columns)} ,{file_df}")

        cursor = self.postgres_conn.cursor()

        cursor.execute(f"{table_cols_query}")
        result = cursor.fetchall()

        self.log.info(f"Table columns: {result[0]}")

        table_cols_str = ','.join([f'"{col}"' for col in result[0][0].split(",")])

        query = f"""
            SELECT {table_cols_str} FROM "{mirror_db}"."{mirror_schema}"."{mirror_table}"
            where "FILE_DATE" = '{run_date}'
            """
        self.log.info(f"Table Data Query:{query}")

        table_df = pd.read_sql(query, self.postgres_conn)
        self.log.info(f"table_df cols {table_df.columns}, {table_df}")

        # Compare DataFrames
        differences = self.compare_dataframes(file_df,table_df)

        # Print Results
        self.log.info("\n Value Differences:")
        self.log.info(differences["value_differences"])

        self.log.info("\n Rows in df1 but not in df2:")
        self.log.info(differences["rows_in_df1_not_in_df2"])

        self.log.info("\n Rows in df2 but not in df1:")
        self.log.info(differences["rows_in_df2_not_in_df1"])


    def execute(self, context):
        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(),pendulum.tz.UTC).strftime('%Y-%m-%d')
        file_path = context['ti'].xcom_pull(key='downloaded_file_path')
        configs_downloaded_tmp_dir,configs = self.get_file_details(dag_run_date)

        file_format_params = configs[self.dataset_name]["mirror"]["file_format_params"]
        delimiter = file_format_params["delimiter"]

        self.compare_file_table_data(dag_run_date,file_path,delimiter)


