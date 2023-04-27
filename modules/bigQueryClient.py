from google.cloud import bigquery
from google.oauth2 import service_account


class BigQueryClient:
    def __init__(self, project_id):
        self.project_id = project_id
        self.client = None

    def connect(self):
        """
        Connect to the BigQuery Client using ADC
            What happens is that the Client will look for the location of the credentials file on your local computer, most likely stored in ~/.config/gcloud/ somewhere
        """
        self.client = bigquery.Client(project=self.project_id)
    
    def execute_query(self, query):
        """
        Execute a query on the connected BigQuery client.
        """
        if not self.client:
            self.connect()

        job_config = bigquery.QueryJobConfig()
        query_job = self.client.query(query, job_config=job_config)
        return query_job.result()
