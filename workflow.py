from google.cloud import bigquery
import pandas

df = pandas.DataFrame(
    {
        'id': [5, 6, 7],
        'first_name': ['Harry', 'Emma', 'Joy'],
        'last_name': ['Potter', 'Waston', 'Phillipe'],
        'address': ['California', 'New York','London'],
        'product_ordered': ['Skirt', 'T-shirt', 'Pants'],
        'expend': [3.5, 4.2, 6.3]
    }
)
client = bigquery.Client()
table_id = 'first_trial.providers'
# Since string columns use the "object" dtype, pass in a (partial) schema
# to ensure the correct BigQuery data type.
job_config = bigquery.LoadJobConfig(schema=[
    bigquery.SchemaField("first_name", "STRING"),
])

job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)

# Wait for the load job to complete.
job.result()