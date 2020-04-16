from google.cloud import bigquery

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="key.json"

client = bigquery.Client()



# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "stackoverflow" dataset
dataset_ref = client.dataset("stackoverflow", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)



#list_of_tables = list(client.list_tables(dataset)) # Your code here
tables = list(client.list_tables(dataset))
list_of_tables = [table.table_id for table in tables]

# Print your answer
print(list_of_tables)


# Construct a reference to the "posts_answers" table
answers_table_ref = dataset_ref.table("comments")

# API request - fetch the table
answers_table = client.get_table(answers_table_ref)

# Preview the first five lines of the "posts_answers" table
answer = client.list_rows(answers_table, max_results=10).to_dataframe()

print(answer)