import os.path
import re
from datetime import datetime
from io import StringIO

import pendulum
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from dateutil.parser import parse


class CopyFileToPostgresOperator(BaseOperator):
    def __init__(self, db_conn_id,table_name,file_format_params,datetime_pattern,encoding, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_format_params = file_format_params
        self.full_table_name = table_name
        self.datetime_pattern = datetime_pattern
        self.encoding = encoding
        self.postgres_conn = PostgresHook(postgres_conn_id=db_conn_id).get_conn()

    def generate_regex_pattern(self,date_format):
        # Mapping of format components to regex patterns
        format_mapping = {
            "YYYY": r"(\d{4})",
            "MM": r"(\d{2})",
            "DD": r"(\d{2})"
        }

        # Escape special characters (- and _)
        pattern = date_format.replace("-", "[-_]").replace("/", "[-_]").replace(".", "[-_]")

        # Replace format placeholders with regex groups
        for key, regex in format_mapping.items():
            pattern = pattern.replace(key, regex)

        return rf"{pattern}"

    def complete_date(self,input_date: str, reference_date: str):
        """
        Parses an input date (year or year-month) and fills missing parts using a reference date.

        Args:
            input_date (str): Date input in format 'yyyy' or 'yyyy-mm'.
            reference_date (str): Another date (yyyy-mm-dd) to take missing month and/or day from.

        Returns:
            str: Completed date in 'yyyy-mm-dd' format.
        """
        try:
            ref_date = parse(reference_date)  # Parse reference date
            parsed_date = parse(input_date, default=datetime(ref_date.year, ref_date.month, ref_date.day))
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            return None  # Invalid input

    def extract_file_date(self,filename):
        # Define regex pattern for yyyy-dd-mm format
        pattern = self.generate_regex_pattern(self.datetime_pattern)

        # Search for date pattern in filename
        match = re.search(pattern, filename)
        python_date_format = self.datetime_pattern.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")

        if match:
            extracted_date = match.group()

            # Validate and format the date
            try:
                valid_date = datetime.strptime(extracted_date, python_date_format)
                return valid_date.strftime(python_date_format)  # Return in the same format
            except ValueError:
                return None  # Invalid date
        else:
            return "No valid date found in filename"

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
    def load_data_to_postgres(self,file_path,dag_run_date ):
        # Extract metadata
        file_name = os.path.basename(file_path)
        file_date = self.extract_file_date(file_name) if self.datetime_pattern else dag_run_date
        username_query = "select current_user"
        current_datetime = datetime.now()
        delimiter = self.file_format_params.get("delimiter",",")
        # line_terminator = "\n"
        encoding = "utf-8"

        # Read file as DataFrame
        df = pd.read_csv(file_path, delimiter=delimiter,  encoding=encoding)

        # Transform column names
        df.columns = self.clean_column_names(df.columns)
        # Add metadata columns
        df["FILE_DATE"] = self.complete_date(file_date,dag_run_date)
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

        self.log.info(f"Table columns from postgres: {pg_columns}")

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
        dag_run_date = datetime.fromtimestamp(context["data_interval_end"].timestamp(), pendulum.tz.UTC).strftime(
            '%Y-%m-%d')
        self.load_data_to_postgres(file_path,dag_run_date)