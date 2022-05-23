from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryExecuteQueryOperator, BigQueryGetDataOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

log = logging.getLogger(__name__)


def my_func1(name):
    print(f'Hello {name}')

def my_func2(name: str):
    print(f'Hello {name}')

default_args = {
    'start_date': datetime(2022, 5, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    'python_operator_sample',
    default_args=default_args,
    description='A simple python operator DAG',
    schedule_interval="0 5 * * *",
    catchup=False,
    tags=['python'],
) as dag:
    hello_my_func1 = PythonOperator(task_id='hello_my_func1', python_callable=my_func1, op_args=['May'])

    hello_my_func2 = PythonOperator(task_id='hello_my_func2', python_callable=my_func2, op_kwargs={"name": "Hoa"})

    read_mysql_customers = MySqlOperator(
        task_id='read_mysql_customers',
        mysql_conn_id="may-mysql",
        sql="select * from customers limit 10"

    )

    mysql_to_gcs = MySQLToGCSOperator(
        task_id='mysql_to_gcs',
        mysql_conn_id="may-mysql",
        sql="select * from customers",
        bucket="mysql_transfer",
        filename="customers.csv",
        export_format='csv'
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket="mysql_transfer",
        source_objects="customers.csv",
        destination_project_dataset_table="mysql_hoa_db.customers",
        write_disposition="WRITE_APPEND"

    )

    test_bq_connection = BigQueryInsertJobOperator(
        task_id='test_bq_connection',
        configuration={
            "query": {
                "query": 'SELECT 1',
                "useLegacySql": False
            }
        }
    )

    execute_bq = BigQueryExecuteQueryOperator(
        task_id='execute_bq',
        sql='select * from first_trial.customers',
    )
    get_data = BigQueryGetDataOperator(
        task_id='get_data',
        dataset_id='first_trial',
        table_id='customers',
        max_results=5,
        selected_fields=['id', 'first_name', 'last_name', 'product_ordered', 'expend']

    )

    [hello_my_func1, hello_my_func2] >> read_mysql_customers >> mysql_to_gcs >> gcs_to_bq >> test_bq_connection >> execute_bq >> get_data