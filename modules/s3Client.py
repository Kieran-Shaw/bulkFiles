import boto3
import urllib.parse
import os

class s3Client:
    def __init__(self,aws_access_key_id,aws_secret_access_key):
        self.client = None
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
    
    def connect(self):
        self.client = boto3.client('s3',self.aws_access_key_id,self.aws_secret_access_key)

    def stringBuild(state,year,quarter,file):
        s3_url = 's3://vericred-emr-workers/production/plans/nava_benefits/csv/small_group/'+state+'/'+year+'/'+quarter+'/'+file+'.csv'
        parsed_url = urllib.parse.urlparse(s3_url)
        bucket_name = parsed_url.netloc
        prefix = parsed_url.path.lstrip('/')
        return bucket_name, prefix
    
    def listObjects(s3Client,bucket_name,prefix):
        response = s3Client.list_objects_v2(Bucket=bucket_name,Prefix=prefix)
        try:
            if len(response['Contents']) >= 1:
                return True
            else:
                return False
        except KeyError:
            return False
    
    def downloadFile(s3Client,bucket_name,prefix,state,year,quarter,file):
        file_path = '/Users/kieranshaw/Documents/Nava Documents/biz_ops_projects/ideon_api/small_group_data/'+state+'_'+year+'_'+quarter+'_'+file+'.csv'
        s3Client.download_file(bucket_name,prefix,file_path)
        if os.path.exists(file_path):
            print(state+'_'+year+'_'+'quarter'"File downloaded successfully.")
        else:
            print("File download failed.")