
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="key.json"

from bq_helper import BigQueryHelper

client = BigQueryHelper('bigquery-public-data', 'stackoverflow')




QUERY = """
    SELECT * FROM `bigquery-public-data.stackoverflow.comments` 
    
    """

print( client.estimate_query_size(QUERY) ) #13.3 GB



bq_assistant = BigQueryHelper('bigquery-public-data', 'new_york')

QUERY = """
        SELECT * FROM `bigquery-public-data.new_york.citibike_trips`
        """

print( bq_assistant.estimate_query_size(QUERY) ) #4.6 GB