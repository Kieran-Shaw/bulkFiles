import boto3
import os

class s3Client:
    def __init__(self,aws_access_key_id:str,aws_secret_access_key:str):
        self.client = None
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
    
    def connect(self):
        self.client = boto3.client('s3',
                                   aws_access_key_id=self.aws_access_key_id,
                                   aws_secret_access_key=self.aws_secret_access_key)
    
    def downloadFile(self,bucket_name:str,prefix:str,local_file_path:str):
        """
        Download the file and check if the path exists
        """
        if not self.client:
            self.connect()
        
        self.client.download_file(bucket_name,prefix,local_file_path)

        if os.path.exists(local_file_path):
            return True
        else:
            raise IOError("Download For File Failed")
        
    def listObjects(self,bucket_name:str,prefix:str):
        """
        List the objects in each states bucket
        """
        if not self.client:
            self.connect()

        response = self.client.list_objects_v2(Bucket=bucket_name,Prefix=prefix)
        return response