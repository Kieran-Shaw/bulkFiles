from modules.s3Client import s3Client
from modules.bigQueryClient import bigQueryClient
import os
import urllib.parse
import pandas as pd


class fileProcessing:
    def __init__(self,current_year:str,current_quarter:str,states_list:list,s3Client:s3Client,bigQueryClient:bigQueryClient):
        self.s3Client = s3Client
        self.bigQueryClient = bigQueryClient
        self.current_year = current_year
        self.current_quarter = current_quarter
        self.states_list = states_list

    def fileProcess(self):
        new_year = fileProcessing.newYearLogic(self)
        if new_year:
            print('New Year! Starting Process...')
            """
            If New Year, we need to do the following:
            1. Download All of the Files from each state
            2. Concat All of the Files for each state
                * for pricings, make sure to append the quarter as a column
            3. Create a new dataset in BigQuery with the following format 'YYYYACAPlans'
            4. Upload all files to the new dataset
            5. Delete the files from local computer
            6. Copy over the rating area file [note - we should ask Ideon to include this in the bulk files, not sure why it is not there]
            """
            eval_status = []
            local_download_list = fileProcessing.yearDownload(self)
            # manipulate the files / concat the files
            local_download_combined_list = fileProcessing.concatFiles(self,local_download_list=local_download_list)
            return
            upload_eval = fileProcessing.yearUpload(self,local_download_concat_list=local_download_combined_list)
            if upload_eval:
                eval = fileProcessing.deleteLocalDownload(local_download_combined_list)
                eval_status.append(eval)
                job = self.bigQueryClient.copyTable(source_dataset=f'{str(int(self.current_year)-1)}ACAPlans',source_table='zip_counties_rating_area',destination_dataset=f'{self.current_year}ACAPlans',destination_table='zip_counties_rating_area')
                if job:
                    eval_status.append(True)
                else:
                    eval_status.append(False)
            else:
                print('Files Not Deleted, Check Downloads')
            
            if all(eval_status):
                print('Complete!')

            return

        elif not new_year:
            print('Not A New Year, Starting Process...')
            """
            If Not New Year, get the pricings for each state, the year, quarter, and pricings file
            Add a column to the pricings file which says what quarter it is
            Concat that file to the pricings table in bigquery
            """
            local_download_list = fileProcessing.quarterDownload(self)
            upload_eval = fileProcessing.quarterUpload(self,local_download_list)
            if upload_eval:
                eval_status = fileProcessing.deleteLocalDownload(local_download_list)
                if eval_status:
                    print('Complete!')
                else:
                    print('Files Not Deleted, Check Downloads')
            return

    def newYearLogic(self):
        if self.current_quarter == 'Q1':
            return True
        else:
            return False
    
    def quarterDownload(self):
        local_download_list = []
        download_eval = []
        for i in range(len(self.states_list)):
            bucket_name, prefix = fileProcessing.stringBuild(self.states_list[i],self.current_year,self.current_quarter,'pricings')
            local_file_path = fileProcessing.localFilePath(self,self.states_list[i])
            download_status = self.s3Client.downloadFile(bucket_name,prefix,local_file_path)
            if download_status:
                download_eval.append(True)
                local_download_list.append(local_file_path)
        if all(download_eval):
            print(f'Downloaded Files: {local_download_list}')
        return local_download_list
    
    def yearDownload(self):
        local_download_list = []
        download_eval = []
        for i in range(len(self.states_list)):
            response = fileProcessing.fileLists3Bucket(self,state=self.states_list[i],year=self.current_year,quarter=self.current_quarter)
            list_of_s3_files = fileProcessing.buildFileList(self,response=response)
            for l in range(len(list_of_s3_files)):
                file_name = fileProcessing.buildFilename(file_string=list_of_s3_files[l])
                local_file_path = fileProcessing.localFilePath(self,state=self.states_list[i],file_name=file_name,specific_file=False)
                bucket_name, prefix = fileProcessing.stringBuild(process_state=self.states_list[i],year=self.current_year,quarter=self.current_quarter,file_name=file_name.split('.')[0])
                download_status = self.s3Client.downloadFile(bucket_name=bucket_name,prefix=prefix,local_file_path=local_file_path)
                if download_status:
                    download_eval.append(True)
                    local_download_list.append(local_file_path)
        if all(download_eval):
            print(f'Downloaded Files: {local_download_list}')
        return local_download_list
    
    def concatFiles(self,local_download_list:list):
        combined_file_path = []
        while local_download_list:
            # pop the index of the first file, get the base_name of the file (ex: pricings.csv), look through list to find the matching file, pop the matching file
            file = local_download_list.pop(0)
            file_base_name = fileProcessing.extractFileName(self,file_path=file)
            matching_file_list = [f for f in local_download_list if file_base_name == fileProcessing.extractFileName(self,file_path=f)]
            local_download_list = [f for f in local_download_list if f not in matching_file_list]
            matching_file_list.append(file)

            # download the files and return a dataframe
            df = fileProcessing.concatCSV(self,matched_files=matching_file_list)

            # save to local computer
            downloads_folder = os.path.join(os.path.expanduser('~'), 'Downloads')
            download_file_path = f'{downloads_folder}/{self.current_year}{self.current_quarter}{file_base_name}_total.csv'
            df.to_csv(download_file_path,index=False)

            # if the file was saved, append it to return_list and delete the files in the matching_file_list
            if os.path.exists(download_file_path):
                combined_file_path.append(download_file_path)
                fileProcessing.deleteLocalDownload(local_file_path_list=matching_file_list)

        return combined_file_path

    def quarterUpload(self,local_download_list:list):
        eval_list = []
        big_query_dataset_name = fileProcessing.buildDatasetName(self)
        table_name = 'pricings'
        for i in range(len(local_download_list)):
            df = fileProcessing.readCSV(self,local_download_list[i])
            job = self.bigQueryClient.appendToTable(df,big_query_dataset_name,table_name)
            if job:
                eval_list.append(True)
        if all(eval_list):
            print(f'Uploaded Files: {local_download_list}')
            return True
        else:
            return False
        
    def yearUpload(self,local_download_concat_list:list):
        eval_list = []
        dataset_id = f'{str(self.current_year)}ACAPlans'

        # create a new dataset & return status, append to status list
        created_result = self.bigQueryClient.createDataset(dataset_id=dataset_id)     
        eval_list.append(created_result)

        # upload those new files from the stored file paths
        for i in range(len(local_download_concat_list)):
            df = pd.read_csv(local_download_concat_list[i])
            # FIX HERE, ONE OF THE DATASETS IS THROWING AN ERROR ON SOME COLUMNS DATA TYPE

            dataset_name = fileProcessing.extractFileName(self,file_path=local_download_concat_list[i])
            dataset_name.replace('_total','')
            job = self.bigQueryClient.createTable(df=df,dataset_name=dataset_id,table_name=dataset_name)
            if job:
                eval_list.append(True)
        
        if all(eval_list):
            print(f'Uploaded Files: {local_download_concat_list}')
            return True
        else:
            return False

    def localFilePath(self,state:str,file_name:str = None,specific_file:bool = True):
        if specific_file:
            downloads_folder = os.path.join(os.path.expanduser('~'), 'Downloads')
            download_file_path = f'{downloads_folder}/{state}{str(self.current_year)}{str(self.current_quarter)}pricings.csv'
        elif not specific_file:
            downloads_folder = os.path.join(os.path.expanduser('~'), 'Downloads')
            download_file_path = f'{downloads_folder}/{state}{str(self.current_year)}{str(self.current_quarter)}{str(file_name)}'
        return download_file_path
    
    def bucketNamePrefix(s3_url:str):
        parsed_url = urllib.parse.urlparse(s3_url)
        bucket_name = parsed_url.netloc
        prefix = parsed_url.path.lstrip('/')
        return bucket_name, prefix
    
    def stringBuild(process_state:str,year:str,quarter:str,file_name:str = None,specific_file:bool = True):
        if specific_file:
            s3_url = f's3://vericred-emr-workers/production/plans/nava_benefits/csv/small_group/{process_state}/{year}/{quarter}/{file_name}.csv'
            bucket_name, prefix = fileProcessing.bucketNamePrefix(s3_url=s3_url)
            return bucket_name, prefix
        elif not specific_file:
            s3_url = f's3://vericred-emr-workers/production/plans/nava_benefits/csv/small_group/{process_state}/{year}/{quarter}'
            bucket_name, prefix = fileProcessing.bucketNamePrefix(s3_url=s3_url)
            return bucket_name, prefix
    
    def buildDatasetName(self):
        dataset_name = f'{str(self.current_year)}ACAPlans'
        return dataset_name
    
    def deleteLocalDownload(local_file_path_list:list):
        eval_list = []
        for i in range(len(local_file_path_list)):
            try:
                os.remove(local_file_path_list[i])
                eval_list.append(True)
            except FileNotFoundError:
                print(f'{local_file_path_list[i]} does not exist')
            except PermissionError:
                print(f'''You don't have the necessary permissions to delete {local_file_path_list[i]}''')
            except Exception as e:
                print(f"An error occurred: {e}")
        if all(eval_list):
            print(f'Files Deleted: {local_file_path_list}')
            return True
        else:
            return False
    
    def readCSV(self,local_file_path:str):
        # find the base name to see == pricings, it so append the quarter column
        file_base_name = fileProcessing.extractFileName(self,file_path=local_file_path)
        if file_base_name == 'pricings':
            try:
                df = pd.read_csv(local_file_path)
                df = fileProcessing.addQuarterColumn(self,df)
                return df
            except Exception as e:
                print(f'An error ocurred: {e}')
        elif file_base_name != 'pricings':
            try:
                df = pd.read_csv(local_file_path)
                return df
            except Exception as e:
                print(f'An error occured: {e}')

    def addQuarterColumn(self,df:pd.DataFrame):
        df['quarter'] = str(self.current_quarter)
        return df
    
    def fileLists3Bucket(self,state:str,year:str,quarter:str):
        bucket_name, prefix = fileProcessing.stringBuild(process_state=state,year=year,quarter=quarter,specific_file=False)
        response = self.s3Client.listObjects(bucket_name=bucket_name,prefix=prefix)
        return response
    
    def buildFileList(self,response:dict):
        file_list = [response['Name']+'/'+response['Contents'][i]['Key'] for i in range(len(response['Contents']))]
        return file_list
    
    def buildFilename(file_string):
        return file_string.split('/')[-1]
    
    def extractFileName(self,file_path:str):
        start_str = self.current_quarter
        end_str = '.csv'
        start_index = file_path.index(start_str) + len(start_str)
        end_index = file_path.index(end_str)
        return file_path[start_index:end_index]
    
    def concatCSV(self,matched_files:list):
        df = pd.DataFrame()
        for i in range(len(matched_files)):
            df_download = fileProcessing.readCSV(self,matched_files[i])
            df = pd.concat([df,df_download],ignore_index=True)
        df.reset_index(drop=True,inplace=True)
        return df
