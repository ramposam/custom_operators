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

from operators.constants import file_schema_query, file_cols_query, postgres_table_schema_query, \
    postgres_col_count_query


class FilePostgresTableDataCheckOperator(BaseOperator):
    def __init__(self, db_conn_id, s3_conn_id=None, bucket_name=None, configs_path=None, table_name=None, dataset_name=None, encoding=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.dataset_name = dataset_name
        self.bucket_name = bucket_name
        self.s3_conn_id = s3_conn_id
        self.encoding = encoding
        self.configs_path = configs_path
        self.postgres_conn = PostgresHook(postgres_conn_id=db_conn_id).get_conn()

    def get_file_details(self,run_date):
        temp_dir = tempfile.mkdtemp()
        local_dir = os.path.join(temp_dir, "configs", self.dataset_name)

        Path(local_dir).mkdir(exist_ok=True,parents=True)
        self.log.info(f"Temporary directory created:{local_dir} to load configs" )

        if self.bucket_name and self.bucket_name != "None":
            # Use S3 to download configs
            s3_folder = f"{os.path.join(self.configs_path, self.dataset_name)}"  # "dataset_configs/dev"
            # Strip S3 protocol and bucket name from prefix if present
            if s3_folder.startswith('s3://'):
                s3_folder = s3_folder[5:]
                if s3_folder.startswith(self.bucket_name + '/'):
                    s3_folder = s3_folder[len(self.bucket_name)+1:]
            s3_utils.download_s3_folder(self.s3_conn_id, self.bucket_name, s3_folder, local_dir)
            self.log.info(f"Configs downloaded from S3 to {local_dir}")
            # Verify files were actually downloaded
            if not any(os.path.exists(os.path.join(local_dir, f)) for f in os.listdir(local_dir)):
                raise Exception(f"No config files found in S3 at prefix '{s3_folder}' in bucket '{self.bucket_name}'. Local directory '{local_dir}' is empty after download attempt.")
        else:
            # Use local config path
            source_dir = os.path.join(self.configs_path, self.dataset_name)
            if os.path.exists(source_dir):
                import shutil
                shutil.copytree(source_dir, local_dir, dirs_exist_ok=True)
                self.log.info(f"Configs copied from local path {source_dir} to {local_dir}")
            else:
                raise Exception(f"Local config path {source_dir} does not exist")

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

        df_diff = None
        if df1.shape != df2.shape:
            self.log.info("DataFrames have different shapes. Comparison may be inconsistent.")
        else:
            df_diff = pd.concat([df1, df2]).drop_duplicates(keep=False)
            self.log.info(df_diff)

        return {
            "differences": df_diff,  # Shows exact cell differences
            # "rows_in_df1_not_in_df2": df1_not_in_df2,  # Rows unique to df1
            # "rows_in_df2_not_in_df1": df2_not_in_df1  # Rows unique to df2
        }

        # Compare DataFrames using pandas built-in method
        # diff = df1.compare(df2, keep_equal=False)
        # diff = diff.rename(columns={'self': 'file', 'other': 'table'})



        # Identify rows that are in df1 but not in df2
        # df1_not_in_df2 = df1[~df1.apply(tuple, axis=1).isin(df2.apply(tuple, axis=1))]

        # Identify rows that are in df2 but not in df1
        # df2_not_in_df1 = df2[~df2.apply(tuple, axis=1).isin(df1.apply(tuple, axis=1))]

    def compare_dicts(self,dict1, dict2):
        differences = {}

        for key in dict1.keys() | dict2.keys():  # Union of keys from both dicts
            val1 = dict1.get(key, {})
            val2 = dict2.get(key, {})

            diff = {}
            for sub_key in val1.keys() | val2.keys():
                sub_val1 = val1.get(sub_key)
                sub_val2 = val2.get(sub_key)
                if sub_val1 != sub_val2:
                    diff[sub_key] = {"dict1": sub_val1, "dict2": sub_val2}

            if diff:
                differences[key] = diff

        return differences



    def compare_file_table_data(self, run_date, file_path, delimiter=","):
        # Query the target table
        # Extract database and schema from connection as defaults
        default_db = self.postgres_conn.info.dbname
        # Query for current schema since psycopg2 ConnectionInfo doesn't have schema attribute
        cursor = self.postgres_conn.cursor()
        cursor.execute("SELECT current_schema()")
        default_schema = cursor.fetchone()[0]
        cursor.close()
        mirror_db = self.table_name.split(".")[0] if "." in self.table_name else default_db
        mirror_schema = self.table_name.split(".")[1] if "." in self.table_name else default_schema
        mirror_table = self.table_name.split(".")[2] if "." in self.table_name else self.table_name
        table_cols_query = postgres_table_schema_query.format(mirror_db=mirror_db,
                                                     mirror_schema=mirror_schema,
                                                     mirror_table=mirror_table)
        self.log.info(f"Table columns query:{table_cols_query}")

        # Load the file data into a DataFrame
        file_df = pd.read_csv(file_path, sep=delimiter, encoding=self.encoding)
        file_df.columns = file_df.columns.str.upper().str.replace(" ", "_")
        self.log.info(f"File cols {list(file_df.columns)} ")

        file_col_cnt_mappings = {}
        for col in file_df.columns:
            file_col_cnt_mappings[col]= {"total_count":file_df[col].count(),"distinct_count":file_df[col].nunique()}

        cursor = self.postgres_conn.cursor()

        cursor.execute(f"{table_cols_query}")
        result = cursor.fetchall()

        table_cols_str = ','.join([f'"{col}"' for col in result[0][0].split(",")])
        self.log.info(f"Table columns: {table_cols_str}")

        col_distinct_count_queries = postgres_col_count_query.format(mirror_db=mirror_db,
                                                 mirror_schema=mirror_schema,
                                                 mirror_table=mirror_table,
                                                 file_date=run_date)

        self.log.info(f"Table Data Query:{col_distinct_count_queries}")
        cursor.execute(f"{col_distinct_count_queries}")
        result = cursor.fetchall()

        table_col_cnt_mappings = {}
        for col, query in result:
            self.log.info(f"column {col} query:{query}")
            cursor.execute(f"{query}")
            col_cnt_result = cursor.fetchall()
            total_rec_cnt = col_cnt_result[0][0]
            distinct_rec_cnt = col_cnt_result[0][1]

            table_col_cnt_mappings[col] = {"total_count": total_rec_cnt, "distinct_count": distinct_rec_cnt}

        self.log.info(f"File column counts:{file_col_cnt_mappings}")
        self.log.info(f"Table column counts:{table_col_cnt_mappings}")

        # Compare DataFrames
        # differences = self.compare_dataframes(file_df,table_df)
        result = self.compare_dicts(file_col_cnt_mappings,table_col_cnt_mappings)

        if len(result)>0:
            self.log.info(f"File and Table has different data counts:{result}")
            raise Exception(f"File and Table has different data counts:{result}")

        else:
            self.log.info(f"File and Table column count are same.")

        # self.log.info("\n Rows in df1 but not in df2:")
        # self.log.info(differences["rows_in_df1_not_in_df2"])

        # self.log.info("\n Rows in df2 but not in df1:")
        # self.log.info(differences["rows_in_df2_not_in_df1"])


    def get_all_upstream_task_ids(self, context):
        """Get all upstream task IDs recursively by traversing the DAG structure in BFS order."""
        dag = context['dag']
        current_task_id = context['task'].task_id
        
        # Get all tasks in the DAG
        all_tasks = {task.task_id: task for task in dag.tasks}
        
        # Find all upstream tasks recursively using BFS to maintain order
        upstream_task_ids = []
        visited = set()
        queue = [current_task_id]
        
        while queue:
            task_id = queue.pop(0)
            if task_id in visited:
                continue
            visited.add(task_id)
            
            task = all_tasks.get(task_id)
            if task:
                for upstream_task_id in task.upstream_task_ids:
                    if upstream_task_id not in visited and upstream_task_id not in upstream_task_ids:
                        upstream_task_ids.append(upstream_task_id)
                        queue.append(upstream_task_id)
        
        return upstream_task_ids

    def execute(self, context):
        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(),pendulum.tz.UTC).strftime('%Y-%m-%d')
        all_upstream_task_ids = self.get_all_upstream_task_ids(context)
        self.log.info(f"All upstream task IDs: {all_upstream_task_ids}")
        file_path = context['ti'].xcom_pull(task_ids=all_upstream_task_ids[2],key='downloaded_file_path')
        self.log.info(f"File path: {file_path}")
        configs_downloaded_tmp_dir,configs = self.get_file_details(dag_run_date)

        file_format_params = configs[self.dataset_name]["mirror"]["file_format_params"]
        delimiter = file_format_params["delimiter"]

        self.compare_file_table_data(dag_run_date,file_path,delimiter)


