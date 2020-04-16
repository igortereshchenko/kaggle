from google.cloud import bigquery
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go


import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="key.json"

client = bigquery.Client()


questions_query = """
                  SELECT id, title, owner_user_id
                      FROM `bigquery-public-data.stackoverflow.posts_questions`
                      WHERE tags LIKE '%bigquery%'
                  """

# Set up the query (cancel the query if it would use too much of
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
questions_query_job =  client.query(questions_query, job_config=safe_config)  # Your code goes here

# API request - run the query, and return a pandas DataFrame
questions_results = questions_query_job.to_dataframe()  # Your code goes here

# Preview results
print(questions_results)

#
# # Your code here
# answers_query = """
#                 SELECT answer.id, answer.body, answer.owner_user_id
#                     FROM `bigquery-public-data.stackoverflow.posts_questions` AS question
#                     INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS answer ON question.id = answer.parent_id
#                 WHERE question.tags LIKE '%bigquery%'
#                 """
#
# # Set up the query
# answers_query_job =  client.query(answers_query)
#
# # API request - run the query, and return a pandas DataFrame
# answers_results = answers_query_job.to_dataframe()
#
# # Preview results
# print(answers_results[['id','body']])



bigquery_experts_query = """
                         SELECT u.display_name AS user_name, COUNT(1) AS number_of_answers
                             FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                             INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a ON q.id = a.parent_Id
                             INNER JOIN `bigquery-public-data.stackoverflow.users` AS u ON u.id = a.owner_user_id
                             WHERE q.tags LIKE '%bigquery%'
                             GROUP BY a.owner_user_id, u.display_name
                             ORDER BY number_of_answers DESC
                             limit 100
                         """

# Set up the query

bigquery_experts_query_job = client.query(bigquery_experts_query)

# API request - run the query, and return a pandas DataFrame
bigquery_experts_results = bigquery_experts_query_job.to_dataframe()

# Preview results
print(bigquery_experts_results)





# Your code here
answers_query = """
                SELECT question.creation_date, count(question.id) as count
                    FROM `bigquery-public-data.stackoverflow.posts_questions` AS question
                WHERE question.tags LIKE '%bigquery%'                    
                GROUP BY question.creation_date
                """

# Set up the query
answers_query_job =  client.query(answers_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
answers_results = answers_query_job.to_dataframe()

# Preview results
print(answers_results)







fig = make_subplots(rows=1, cols=2)

fig.add_trace(
    go.Scatter(x=answers_results['creation_date'], y=answers_results['count']),
    row=1, col=1
)

fig.add_trace(
    go.Bar(x=bigquery_experts_results.user_name, y=bigquery_experts_results.number_of_answers),
    row=1, col=2
)

fig.update_layout(height=600, width=800, title_text="Info")
fig.show()