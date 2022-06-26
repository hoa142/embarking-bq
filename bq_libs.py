"""Libraries to interact with PostgreSQL and Google Cloud BigQuery."""

import os

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from sqlalchemy import create_engine

load_dotenv()
bq_client = bigquery.Client()
pg_db_url = os.environ["ENGINE_STRING"]
engine = create_engine(pg_db_url)


def load_pg_table_to_bq(
    pg_schema: str,
    pg_table: str,
    bq_dataset: str,
    bq_table: str,
    write_mode: str = "WRITE_EMPTY",
):
    """
    Full load PostgreSQL table to initialize Google Cloud BigQuery table.

    :param pg_schema: PostgreSQL schema name
    :param pg_table: PostgreSQL table name
    :param bq_dataset: BigQuery dataset name
    :param bq_table: BigQuery table name
    :param write_mode: writing mode
        WRITE_TRUNCATE: If the table already exists, BigQuery overwrites the table
                        data and uses the schema from the load.
        WRITE_APPEND: If the table already exists, BigQuery appends the data to the table.
        WRITE_EMPTY: If the table already exists and contains data, a 'duplicate' error is returned to the job result.
    :return: Google Cloud BigQuery LoadJob
    """
    pg_table_df = pd.read_sql_table(pg_table, engine, pg_schema)

    bq_table_id = f"{bq_dataset}.{bq_table}"

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = write_mode

    job = bq_client.load_table_from_dataframe(
        pg_table_df, bq_table_id, job_config=job_config
    )

    return job.result()


def append_pg_table_to_bq(
    pg_schema: str,
    pg_table: str,
    bq_dataset: str,
    bq_table: str,
    filtered_column_name: str,
    incremental_period: str,
    write_mode: str = "WRITE_APPEND",
):
    """
    Insert incrementally data from PostgreSQL table to Google Cloud BigQuery table.

    :param pg_schema: PostgreSQL schema name
    :param pg_table: PostgreSQL table name
    :param bq_dataset: BigQuery dataset name
    :param bq_table: BigQuery table name
    :param filtered_column_name: PostgreSql table column name where the filter applied
    :param incremental_period: e.g. '1 hour'
    :param write_mode: writing mode
        WRITE_TRUNCATE: If the table already exists, BigQuery overwrites the table
                        data and uses the schema from the load.
        WRITE_APPEND: If the table already exists, BigQuery appends the data to the table.
        WRITE_EMPTY: If the table already exists and contains data, a 'duplicate' error is r
    :return: Google Cloud BigQuery LoadJob
    """
    query = (
        f"select * from {pg_schema}.{pg_table} where "
        f"{filtered_column_name} >= now() - interval '{incremental_period}'"
    )
    pg_table_df = pd.read_sql_query(query, engine)

    bq_table_id = f"{bq_dataset}.{bq_table}"

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = write_mode

    job = bq_client.load_table_from_dataframe(
        pg_table_df, bq_table_id, job_config=job_config
    )

    return job.result()
