from google.cloud import bigquery

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="key.json"

client = bigquery.Client()



# Perform a query.
QUERY = """
    SELECT text FROM `bigquery-public-data.stackoverflow.comments` 
    WHERE user_display_name = "user113397" 
    LIMIT 100
    """


query_job = client.query(QUERY)  # API request


rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.text)