from google.cloud import bigquery

bqclient = bigquery.Client()

# Download query results.
insert_query = """
INSERT INTO first_trial.customers (id, first_name, last_name, address, product_ordered, expend) VALUES (1, 'Jenny', 'Kane', 'hcm', 'string', 10.1);
"""

select_query = """
select * from first_trial.customers
"""

dataframe = (
    bqclient.query(insert_query)
    .result()
    .to_dataframe(
        # Optionally, explicitly request to use the BigQuery Storage API. As of
        # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
        # API is used by default.
        create_bqstorage_client=True,
    )
)
#print(dataframe.head())

# print(type(insert_query))


