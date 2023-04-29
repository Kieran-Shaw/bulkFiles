from modules.fileProcessing import fileProcessing
from modules.s3Client import s3Client
from modules.bigQueryClient import bigQueryClient
import pandas as pd
import os
import datetime
from dotenv import load_dotenv

### ENV VARIABLES ###
load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
PROJECT_ID = os.getenv('PROJECT_ID')

### SETTING CONFIG VARIABLES
CURRENT_YEAR = '2023'
CURRENT_QUARTER = 'Q1'
# CURRENT_YEAR = datetime.datetime.now().strftime('%Y')
# CURRENT_QUARTER = 'Q'+str((datetime.datetime.now().month - 1) // 3 + 1)
STATES_LIST = ['CA','NY']

### INSTANTIATE CLASSES
s3_client = s3Client(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
big_query_client = bigQueryClient(PROJECT_ID)
file_process = fileProcessing(current_year=CURRENT_YEAR,current_quarter=CURRENT_QUARTER,states_list=STATES_LIST,s3Client=s3_client,bigQueryClient=big_query_client)

### RUN CODE
file_process.fileProcess()