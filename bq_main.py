from bq_libs import load_pg_table_to_bq, append_pg_table_to_bq

print(append_pg_table_to_bq("public", "film", "first_trial", "film", "last_update", "1 hour"))