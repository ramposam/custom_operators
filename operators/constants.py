mirror_file_meta_cols = ["filename","file_row_number","file_last_modified"]
mirror_meta_cols = ["created_dts","created_by"]
stage_file_meta_cols = ["filename","file_row_number","file_last_modified"]
stage_meta_cols = ["created_dts","created_by","updated_dts","updated_by","active",
                   "effective_start_date", "effective_end_date","row_hash_id" ]

table_schema_query = """
SELECT LISTAGG(COLUMN_NAME,',') WITHIN GROUP(ORDER BY ORDINAL_POSITION)  AS COLS
FROM {mirror_db}.INFORMATION_SCHEMA.COLUMNS C
WHERE TABLE_SCHEMA ='{mirror_schema}'
AND TABLE_NAME = '{mirror_table}'
AND COLUMN_NAME NOT IN ('CREATED_BY','CREATED_DTS','FILE_DATE','FILE_LAST_MODIFIED','FILE_ROW_NUMBER','FILENAME',
'ROW_HASH_ID','UNIQUE_HASH_ID','UPDATED_DTS','UPDATED_BY')
"""

file_cols_query ="""
SELECT {query_select_cols_str}
FROM @{stage_name}
(FILE_FORMAT => '{file_format_name}')  ;
"""

file_schema_query ="""
SELECT
    count(*) as col_cnt
    FROM TABLE(INFER_SCHEMA(LOCATION=>'@{stage_name}', FILE_FORMAT=>'{file_format_name}')) AS T
ORDER BY 
    T.ORDER_ID ASC;
    """