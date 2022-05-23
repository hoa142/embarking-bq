from google.cloud import bigquery

bqclient = bigquery.Client()

insert_query = """INSERT INTO second_step_film.film_genre
SELECT 
  category_id,
  name,
  ARRAY_AGG(STRUCT(film_id, title, description, release_year, language_id, rental_duration, rental_rate, length, replacement_cost, rating, last_update, special_features, fulltext)) as film
FROM (SELECT f.*, c.category_id, c.name
FROM first_trial.category AS C
FULL OUTER JOIN first_trial.film_category AS fc ON fc.category_id = c.category_id
FULL OUTER JOIN first_trial.film AS f ON f.film_id = fc.film_id
WHERE CAST(f.last_update AS timestamp) > CURRENT_TIMESTAMP() - INTERVAL 1 HOUR) as film_join
GROUP BY category_id, name;"""


dataframe = (
    bqclient.query(insert_query)
    .result()
    .to_dataframe(
        create_bqstorage_client=True,
    )
)

print(dataframe.head())

