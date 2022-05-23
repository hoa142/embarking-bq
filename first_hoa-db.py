import os

import pandas
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery

from dotenv import load_dotenv
load_dotenv()


SELECT_FILM = "SELECT * FROM public.film;"


connection = psycopg2.connect(os.environ["DATABASE_URL"])



# with connection:
#     with connection.cursor() as cursor:
#         cursor.execute(SELECT_FILM)
#         result = cursor.fetchone()


# print(type(result))
# print(result)
#
# print(connection)


########

engine_string = os.environ["ENGINE_STRING"]
engine = create_engine(engine_string)

# df = pd.read_sql_table('film',engine)
#
# client = bigquery.Client()
# table_id = 'first_trial.film'
# # Since string columns use the "object" dtype, pass in a (partial) schema
# # to ensure the correct BigQuery data type.
# job_config = bigquery.LoadJobConfig(schema=[
#     bigquery.SchemaField("title", "STRING"),
# ])
#
# job = client.load_table_from_dataframe(
#     df, table_id, job_config=job_config
# )
#
# # Wait for the load job to complete.
# job.result()


def load_table_to_bq(data_set: str, table_name: str, write_mode="WRITE_EMPTY"):  # If the table already exists and contains data, a 'duplicate error is returned in the job result.
    df = pd.read_sql_table(table_name, engine)
    client = bigquery.Client()
    table_id = f"{data_set}.{table_name}"
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = f"{write_mode}"


    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    # Wait for the load job to complete.
    return job.result()

#print(load_table_to_bq('first_trial', 'category')) # WRITE_APPEND or WRITE_TRUNCATE



def add_data(data_set: str, table_name: str, write_mode="WRITE_APPEND"):
    df = pd.read_sql_query("""select * from film_category where last_update >= now() - interval '1 hour'""", engine)
    client = bigquery.Client()
    table_id = f"{data_set}.{table_name}"
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = f"{write_mode}"

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    # Wait for the load job to complete.
    return job.result()

print(add_data('first_trial', 'film_category', 'WRITE_APPEND'))
















