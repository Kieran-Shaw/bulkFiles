from google.cloud import bigquery
import pandas as pd

class bigQueryClient:
    def __init__(self, project_id:str):
        self.project_id = project_id
        self.client = None

    def connect(self):
        """
        Connect to the BigQuery Client using ADC
            What happens is that the Client will look for the location of the credentials file on your local computer, most likely stored in ~/.config/gcloud/ somewhere
        """
        self.client = bigquery.Client(project=self.project_id)
    
    def appendToTable(self,df:pd.DataFrame,dataset_name:str,table_name:str,write_disposition:str = 'WRITE_APPEND'):
        """
        Append to table in the database that already exists. the to_gbq looks to the credentials placed by aut
        """
        if not self.client:
            self.connect()
        
        table_id = f'{self.project_id}.{dataset_name}.{table_name}'
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=write_disposition
        )
        try:
            job = self.client.load_table_from_dataframe(dataframe=df,destination=table_id,job_config=job_config)
            print(f'Appended {table_name} to {dataset_name}!')
            return job.result()
        except Exception as e:
            print(f'An error occured: {e}')
    
    def createTable(self,df:pd.DataFrame,dataset_name:str,table_name:str):
        """
        Create a new table in a dataset that already exists
        """
        if not self.client:
            self.connect()

        table_id = f'{self.project_id}.{dataset_name}.{table_name}'
        job_config = bigquery.LoadJobConfig(
            autodetect=True
        )
        try:
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            print(f'Created {table_name} & {len(df.index)} Rows, Dataset Name: {dataset_name}')
            return job.result()
        except Exception as e:
            print(f'An error occured: {e}')
    
    def copyTable(self,source_dataset:str,source_table:str,destination_dataset:str,destination_table:str):
        """
        Copy a table and place it in the destination table
        """
        if not self.client:
            self.connect()

        source_table_id = f'{self.project_id}.{source_dataset}.{source_table}'
        destination_table_id = f'{self.project_id}.{destination_dataset}.{destination_table}'
        try:
            job = self.client.copy_table(source_table_id, destination_table_id)
            print(f'Table {source_table} Copied to {destination_dataset} Dataset')
            return job.result()
        except Exception as e:
            print(f'An error occured: {e}')

    def createDataset(self,dataset_id:str):
        """
        Create a new dataset in Bigquery wihtin a project
        """
        if not self.client:
            self.connect()

        dataset_id = f'{dataset_id}'
        try:
            self.client.create_dataset(dataset_id)
            print(f'New Year Dataset Created: {dataset_id}')
            return True
        except Exception as e:
            print(f'An error occured: {e}')
            return False
        



