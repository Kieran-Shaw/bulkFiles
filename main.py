from modules.fileProcessing import fileProcessing
from modules.s3Client import s3Client
from modules.bigQueryClient import bigQueryClient
import datetime
import pandas as pd
import os
from dotenv import load_dotenv

### ENV VARIABLES ###
load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
PROJECT_ID = os.getenv('PROJECT_ID')

### SETTING CONFIG VARIABLES
CURRENT_YEAR = datetime.datetime.now().strftime('%Y')
# CURRENT_QUARTER = 'Q'+str((datetime.datetime.now().month - 1) // 3 + 1)
# CURRENT_YEAR = '2023'
CURRENT_QUARTER = 'Q3'
STATES_LIST = ['CA','NY']

### INSTANTIATE CLASSES
s3_client = s3Client(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
big_query_client = bigQueryClient(PROJECT_ID)
file_process = fileProcessing(current_year=CURRENT_YEAR,current_quarter=CURRENT_QUARTER,states_list=STATES_LIST,s3Client=s3_client,bigQueryClient=big_query_client)

### RUN CODE
file_process.fileProcess()

"""
1. Not New Year
    * Download Pricings & Plans Files
    * Concat the Pricings and Plans Files into one Each
    * Upload those to the correct Table
"""