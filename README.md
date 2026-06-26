# Custom Operators Library

A comprehensive collection of Apache Airflow custom operators designed for building data pipelines with multi-layer architecture (Mirror and Stage layers). This library integrates seamlessly with dbt-based data transformation workflows and supports both PostgreSQL and Snowflake data warehouses.

## Overview

This library provides reusable Airflow operators that automate data movement across pipeline layers:
- **Mirror Layer**: Raw data ingestion with append-only storage for historical tracking
- **Stage Layer**: Transformed data with SCD Type 2 capabilities for change tracking

The operators work in conjunction with `generate_models.py` from the dbt_postgres pipeline to generate dbt models from configurations and execute data transformations.

## Architecture

### Pipeline Flow

```
Source Files → Mirror Layer (Raw) → Stage Layer (Transformed + SCD Type 2)
     ↓              ↓                        ↓
  Acquisition    Mirror Load            Stage Load
     ↓              ↓                        ↓
  Download       dbt Run                dbt Run
     ↓              ↓                        ↓
  Schema Check   Mirror Tests           Stage Tests
```

### Operator Categories

1. **File Acquisition Operators**: Handle file discovery and download from S3 or local filesystem
2. **Data Validation Operators**: Validate schema and data integrity before loading
3. **Data Loading Operators**: Load data into PostgreSQL or Snowflake
4. **dbt Execution Operators**: Execute dbt commands for model generation and testing
5. **Snowflake-Specific Operators**: Handle Snowflake-specific data operations

## Installation

### Prerequisites

- Python 3.8+
- Apache Airflow 2.x
- PostgreSQL or Snowflake
- dbt 1.11+

### Setup

1. **Clone the repository:**
```bash
git clone <repository-url>
cd custom_operators
```

2. **Install the package:**
```bash
pip install -e .
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Configure Airflow:**
Ensure the operators are available in your Airflow environment by adding the package to your Airflow `PYTHONPATH`.

## Available Operators

### File Acquisition Operators

#### AcquisitionOperator
Checks for file presence in S3 or local filesystem based on datetime patterns.

**Parameters:**
- `s3_conn_id`: Airflow S3 connection ID
- `bucket_name`: S3 bucket name
- `dataset_dir`: Dataset directory/prefix
- `file_pattern`: File pattern to match
- `datetime_pattern`: Datetime pattern for file matching

**Usage:**
```python
acq_task = AcquisitionOperator(
    task_id='acq_task',
    s3_conn_id='aws_default',
    bucket_name='my-bucket',
    dataset_dir='datasets/netflix',
    file_pattern='Netflix_Movies_and_TV_Shows.csv',
    datetime_pattern='%Y-%m-%d'
)
```

#### DownloadOperator
Downloads files from S3 or copies from local filesystem to staging area.

**Parameters:**
- `s3_conn_id`: Airflow S3 connection ID
- `bucket_name`: S3 bucket name
- `dataset_dir`: Dataset directory/prefix
- `file_name`: Specific file to download
- `datetime_pattern`: Datetime pattern for file matching

### Data Validation Operators

#### FilePostgresTableSchemaCheckOperator
Validates PostgreSQL table schema against configuration file.

**Parameters:**
- `db_conn_id`: Airflow PostgreSQL connection ID
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

#### FilePostgresTableDataCheckOperator
Validates data integrity in PostgreSQL tables (row counts, null checks).

**Parameters:**
- `db_conn_id`: Airflow PostgreSQL connection ID
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

#### FileSnowflakeTableSchemaCheckOperator
Validates Snowflake table schema against configuration file.

**Parameters:**
- `db_conn_id`: Airflow Snowflake connection ID
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

#### FileSnowflakeTableDataCheckOperator
Validates data integrity in Snowflake tables.

**Parameters:**
- `db_conn_id`: Airflow Snowflake connection ID
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

### Data Loading Operators

#### CopyFileToPostgresOperator
Loads CSV files into PostgreSQL tables.

**Parameters:**
- `db_conn_id`: Airflow PostgreSQL connection ID
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

#### MoveFileToSnowflakeOperator
Loads files into Snowflake stages.

**Parameters:**
- `s3_conn_id`: Airflow S3 connection ID
- `db_conn_id`: Airflow Snowflake connection ID
- `bucket_name`: S3 bucket name
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

### dbt Execution Operators

#### PostgresLoadToMirrorOperator
Generates dbt models and executes dbt run for Mirror layer (PostgreSQL).

**Parameters:**
- `s3_conn_id`: Airflow S3 connection ID
- `db_conn_id`: Airflow PostgreSQL connection ID
- `bucket_name`: S3 bucket name
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

**Usage:**
```python
postgres_mirror_task = PostgresLoadToMirrorOperator(
    task_id='postgres_mirror_task',
    s3_conn_id='aws_default',
    db_conn_id='postgres_default',
    bucket_name='my-bucket',
    configs_path='/opt/airflow/dataset_configs/dev/',
    dataset_name='netflix_movies_and_tv_shows'
)
```

#### PostgresLoadToStageOperator
Generates dbt models and executes dbt run for Stage layer with SCD Type 2 (PostgreSQL).

**Parameters:**
- `s3_conn_id`: Airflow S3 connection ID
- `db_conn_id`: Airflow PostgreSQL connection ID
- `bucket_name`: S3 bucket name
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

**Note:** Automatically sets materialization to `scd_type_2` for historical tracking.

#### PostgresMirrorTestsOperator
Executes dbt tests for Mirror layer (PostgreSQL).

**Parameters:**
- `s3_conn_id`: Airflow S3 connection ID
- `db_conn_id`: Airflow PostgreSQL connection ID
- `bucket_name`: S3 bucket name
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

#### PostgresStageTestsOperator
Executes dbt tests for Stage layer (PostgreSQL).

**Parameters:**
- `s3_conn_id`: Airflow S3 connection ID
- `db_conn_id`: Airflow PostgreSQL connection ID
- `bucket_name`: S3 bucket name
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

#### SnowflakeLoadToMirrorOperator
Generates dbt models and executes dbt run for Mirror layer (Snowflake).

**Parameters:**
- `s3_conn_id`: Airflow S3 connection ID
- `db_conn_id`: Airflow Snowflake connection ID
- `bucket_name`: S3 bucket name
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

#### SnowflakeLoadToStageOperator
Generates dbt models and executes dbt run for Stage layer with SCD Type 2 (Snowflake).

**Parameters:**
- `s3_conn_id`: Airflow S3 connection ID
- `db_conn_id`: Airflow Snowflake connection ID
- `bucket_name`: S3 bucket name
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

**Note:** Automatically sets materialization to `scd_type_2` for historical tracking.

#### SnowflakeMirrorTestsOperator
Executes dbt tests for Mirror layer (Snowflake).

**Parameters:**
- `s3_conn_id`: Airflow S3 connection ID
- `db_conn_id`: Airflow Snowflake connection ID
- `bucket_name`: S3 bucket name
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

#### SnowflakeStageTestsOperator
Executes dbt tests for Stage layer (Snowflake).

**Parameters:**
- `s3_conn_id`: Airflow S3 connection ID
- `db_conn_id`: Airflow Snowflake connection ID
- `bucket_name`: S3 bucket name
- `configs_path`: Path to configuration files
- `dataset_name`: Dataset name

### Snowflake-Specific Operators

#### SnowflakeCopyOperator
Handles Snowflake-specific COPY operations for data loading.

## Integration with dbt_postgres Pipeline

This library is designed to work with the `dbt_postgres` pipeline framework:

### generate_models.py Integration

The dbt execution operators use `generate_models.py` to:
1. Download configuration files from S3 or local filesystem
2. Parse dataset configurations
3. Generate dbt models based on layer (mirror/stage)
4. Execute dbt commands for model creation and testing

**Example dbt command generated:**
```bash
cd /opt/airflow/dbt/ && dbt run --select tag:netflix_movies_and_tv_shows-stage --vars "{'run_date': '2026-06-25', 'materialization': 'scd_type_2'}"
```

### SCD Type 2 Implementation

Stage layer operators automatically configure SCD Type 2 materialization:
- **UNIQUE_HASH_ID**: Hash of unique keys for record matching
- **ROW_HASH_ID**: Hash of all data columns for change detection
- **EFFECTIVE_START_DATE**: Record effective start date
- **EFFECTIVE_END_DATE**: Record effective end date
- **ACTIVE_FL**: Active flag ('Y'/'N')
- **DELETE_FL**: Delete flag

## Example DAG

```python
from airflow import DAG
from datetime import datetime
from operators import (
    AcquisitionOperator,
    DownloadOperator,
    FilePostgresTableSchemaCheckOperator,
    CopyFileToPostgresOperator,
    PostgresLoadToMirrorOperator,
    PostgresMirrorTestsOperator,
    PostgresLoadToStageOperator,
    PostgresStageTestsOperator
)

with DAG(
    'netflix_pipeline',
    schedule_interval='0 23 * * 1-5',
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:
    
    acq_task = AcquisitionOperator(
        task_id='acq_task',
        s3_conn_id='aws_default',
        bucket_name='my-bucket',
        dataset_dir='datasets/netflix',
        file_pattern='Netflix_Movies_and_TV_Shows.csv'
    )
    
    download_task = DownloadOperator(
        task_id='download_task',
        s3_conn_id='aws_default',
        bucket_name='my-bucket',
        dataset_dir='datasets/netflix',
        file_pattern='Netflix_Movies_and_TV_Shows.csv'
    )
    
    schema_check_task = FilePostgresTableSchemaCheckOperator(
        task_id='postgres_schema_check_task',
        db_conn_id='postgres_default',
        configs_path='/opt/airflow/dataset_configs/dev/',
        dataset_name='netflix_movies_and_tv_shows'
    )
    
    copy_task = CopyFileToPostgresOperator(
        task_id='copy_to_postgres_task',
        db_conn_id='postgres_default',
        configs_path='/opt/airflow/dataset_configs/dev/',
        dataset_name='netflix_movies_and_tv_shows'
    )
    
    mirror_task = PostgresLoadToMirrorOperator(
        task_id='postgres_mirror_task',
        s3_conn_id='aws_default',
        db_conn_id='postgres_default',
        bucket_name='my-bucket',
        configs_path='/opt/airflow/dataset_configs/dev/',
        dataset_name='netflix_movies_and_tv_shows'
    )
    
    mirror_tests_task = PostgresMirrorTestsOperator(
        task_id='postgres_mirror_tests_task',
        s3_conn_id='aws_default',
        db_conn_id='postgres_default',
        bucket_name='my-bucket',
        configs_path='/opt/airflow/dataset_configs/dev/',
        dataset_name='netflix_movies_and_tv_shows'
    )
    
    stage_task = PostgresLoadToStageOperator(
        task_id='postgres_stage_task',
        s3_conn_id='aws_default',
        db_conn_id='postgres_default',
        bucket_name='my-bucket',
        configs_path='/opt/airflow/dataset_configs/dev/',
        dataset_name='netflix_movies_and_tv_shows'
    )
    
    stage_tests_task = PostgresStageTestsOperator(
        task_id='postgres_stage_tests_task',
        s3_conn_id='aws_default',
        db_conn_id='postgres_default',
        bucket_name='my-bucket',
        configs_path='/opt/airflow/dataset_configs/dev/',
        dataset_name='netflix_movies_and_tv_shows'
    )
    
    acq_task >> download_task >> schema_check_task >> copy_task >> mirror_task >> mirror_tests_task >> stage_task >> stage_tests_task
```

## Environment Variables

Operators automatically set database environment variables from Airflow connections:

### PostgreSQL
- `POSTGRES_USER`: Database username
- `POSTGRES_PASSWORD`: Database password
- `POSTGRES_HOST`: Database host
- `POSTGRES_DATABASE`: Database name

### Snowflake
- `SNOWFLAKE_USER`: Snowflake username
- `SNOWFLAKE_PASSWORD`: Snowflake password
- `SNOWFLAKE_ACCOUNT`: Snowflake account
- `SNOWFLAKE_DATABASE`: Snowflake database
- `SNOWFLAKE_SCHEMA`: Snowflake schema
- `SNOWFLAKE_WAREHOUSE`: Snowflake warehouse (from extra)
- `SNOWFLAKE_ROLE`: Snowflake role (from extra)

## Configuration

### Airflow Connections

Configure the following Airflow connections:

**PostgreSQL Connection:**
- Conn Id: `postgres_default`
- Conn Type: `Postgres`
- Host: Your PostgreSQL host
- Schema: Database name
- Login: Username
- Password: Password
- Port: 5432

**Snowflake Connection:**
- Conn Id: `snowflake_default`
- Conn Type: `Snowflake`
- Host: Account identifier (e.g., `account.region.snowflakecomputing.com`)
- Schema: Schema name
- Login: Username
- Password: Password
- Extra: `{"warehouse": "COMPUTE_WH", "role": "ACCOUNTADMIN"}`

**AWS S3 Connection:**
- Conn Id: `aws_default`
- Conn Type: `Amazon Web Services`
- Login: AWS Access Key ID
- Password: AWS Secret Access Key

## Troubleshooting

### Common Issues

1. **Command execution fails despite success**
   - Ensure `process.returncode == 0` check is implemented in `execute_dbt_command`
   - Check that `stderr=subprocess.STDOUT` is used for proper error capture

2. **Missing dataset_dir attribute**
   - Ensure `dataset_dir` parameter is included in operator `__init__` if using local filesystem
   - Use `configs_path` instead for S3-based workflows

3. **Environment variables not set**
   - Verify Airflow connection IDs are correct
   - Check that connection extra fields are properly configured for Snowflake

## Dependencies

- `apache-airflow` >= 2.0
- `apache-airflow-providers-amazon` >= 9.0
- `apache-airflow-providers-postgres` >= 5.0
- `apache-airflow-providers-snowflake` >= 6.0
- `boto3` >= 1.35
- `pendulum` >= 2.0
- `core_utils` (external library for config reading and S3 utilities)

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

[Your License Here]

## Support

For issues and questions, please open an issue on the GitHub repository.
