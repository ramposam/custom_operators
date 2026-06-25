import os.path
import shutil

import snowflake.connector
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class MoveFileToSnowflakeOperator(BaseOperator):
    def __init__(self, db_conn_id, stage_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stage_name = stage_name
        self.sf_conn = SnowflakeHook(snowflake_conn_id=db_conn_id).get_conn()


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

        all_upstream_task_ids = self.get_all_upstream_task_ids(context)
        file_path = context['ti'].xcom_pull(task_ids=all_upstream_task_ids[1],key='downloaded_file_path')
        cursor = self.sf_conn.cursor()
        file_name = os.path.basename(file_path)

        duplicate_file_path = os.path.join(os.path.dirname(file_path),f"duplicate_{file_name}")

        self.log.info(f"Duplicating file {file_path} to {duplicate_file_path} for later use.")

        shutil.copy(file_path, duplicate_file_path)
        self.log.info(f"Successfully copied to  {duplicate_file_path}.")

        context['ti'].xcom_push(key='downloaded_file_path_duplicate', value=duplicate_file_path)

        cursor.execute(f"PUT file://{file_path} @{self.stage_name}")
        self.log.info(f"File {file_path} loaded to stage {self.stage_name}.")