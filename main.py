from modules.bigQueryClient import BigQueryClient
import pandas as pd



bq_client = BigQueryClient('small-group-quote')
query = """
with tmp as (
  select *
  from `small-group-quote.2023_q2.zip_counties_rating_area`
  where zip_code = 95030 /* variable for zip_code */
)
select *
from `small-group-quote.2023_q2.pricings` as p
join tmp on p.rating_area_id = tmp.rating_area_id
where p.plan_id = '40513CA0000002-77'
"""
query_result = bq_client.execute_query(query)

rows = []
for row in query_result:
    rows.append(dict(row))

# Convert the list of rows into a dataframe
df = pd.DataFrame(rows)
print(df.head(5))