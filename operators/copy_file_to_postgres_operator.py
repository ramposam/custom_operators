import os.path
import re
from datetime import datetime
from io import StringIO

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

class CopyFileToPostgresOperator(BaseOperator):
    def __init__(self, db_conn_id,table_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_format_params = kwargs.get("file_format", None)
        self.full_table_name = table_name
        self.postgres_conn = PostgresHook(postgres_conn_id=db_conn_id).get_conn()

    # Function to extract date from filename (supports multiple formats)
    def extract_file_date(self,file_name):
        # Define regex patterns for various date formats
        date_patterns = [
            r"(\d{4}[-_]\d{2}[-_]\d{2})",  # YYYY-MM-DD or YYYY_MM_DD
            r"(\d{2}[-_]\d{2}[-_]\d{4})",  # DD-MM-YYYY or DD_MM_YYYY
            r"(\d{8})",  # YYYYMMDD or DDMMYYYY (need validation)
        ]

        for pattern in date_patterns:
            match = re.search(pattern, file_name)
            if match:
                date_str = match.group(1)
                try:
                    # Try parsing automatically with different formats
                    return pd.to_datetime(date_str, errors="coerce", dayfirst=True)
                except Exception:
                    continue  # Try next pattern if parsing fails

        return None  # Return None if no date found

    # Function to clean column names
    def clean_column_names(self,columns):
        return [col.replace(" ", "_").upper() for col in columns]

    # Function to clean column names
    def get_connection_user(self,cur,query):
        try:
            cur.execute(query)
            result = cur.fetchall()
            return result[0][0]
        except:
            return None

    # Function to load data into PostgreSQL using column mapping
    def load_data_to_postgres(self,file_path ):
        # Extract metadata
        file_name = os.path.basename(file_path)
        file_date = self.extract_file_date(file_name)
        username_query = "select current_user"
        current_datetime = datetime.now()
        delimiter = self.file_format_params.get("delimiter",",")
        line_terminator = "\n"
        encoding = "utf-8"

        # Read file as DataFrame
        df = pd.read_csv(file_path, delimiter=delimiter, lineterminator=line_terminator, encoding=encoding)

        # Transform column names
        df.columns = self.clean_column_names(df.columns)
        # Add metadata columns
        df["FILE_DATE"] = file_date
        df["FILE_NAME"] = file_name

        df["CREATED_DTS"] = current_datetime

        # Connect to PostgreSQL
        cur = self.postgres_conn.cursor()

        username = self.get_connection_user(cur,username_query)
        df["CREATED_BY"] = username

        table_name_split = self.full_table_name.split(".")

        schema_name = table_name_split[1]
        table_name = table_name_split[2]

        # Get column names dynamically from PostgreSQL
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                    (schema_name,table_name))
        pg_columns = [row[0].upper() for row in cur.fetchall()]

        # Map dataframe columns to PostgreSQL table columns (ignoring order)
        df = df[[col for col in df.columns if col in pg_columns]]

        # Use COPY FROM for efficient bulk insert
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep=delimiter)
        buffer.seek(0)

        cols = [col.replace(" ", "_").upper() for col in df.columns]
        self.log.info(f"File columns: {cols}")

        cols = [f'"{col}"' for col in cols]
        copy_sql = f"""
            COPY "{schema_name}"."{table_name}" ({', '.join(cols)}) 
            FROM STDIN WITH CSV DELIMITER '{delimiter}' NULL '';
        """
        self.log.info(f"Copy sql: {copy_sql}")

        cur.copy_expert(copy_sql, buffer)
        self.postgres_conn.commit()

        self.log.info(f"Successfully loaded {len(df)} records from {file_name} into {self.full_table_name}")

    def execute(self, context):

        file_path = context['ti'].xcom_pull(key='downloaded_file_path')

        self.load_data_to_postgres(file_path)