## Bulk Files
### Overview
Taking the bulk files from Ideon s3 bucket, processing those files (new year, not new year, concat files, etc.), and then sending those files to BigQuery so that we can build quoting/spreading for our small group team

### Process to Run
requirements.txt - run the requirements.txt file to download the libraries
s3Client - one needs to upload the AWS_ACCESS_KEY_ID and the AWS_SECRET_ACCESS_KEY in the .env file
bigQuery - one needs to place the PROJECT_ID in the .env file, and also go through the Google Application Default Credentials flow
* you must have the Google Cloud SDK installed: https://cloud.google.com/sdk/docs/install
    * I would recommend that you follow the default locations / prompts that come up [note, restarting your terminal / spinning up a new one lets the PATH variable in the .rc (zsh or bash) take hold]
    * if you were not prompted to log in, run from terminal `gcloud auth login`
    * if there are other issues, you can troubleshoot online - they have okay docs
* next, run this from your terminal: `gcloud auth application-default login`
    * it will take you through an auth flow and then store the credentials in your ~/.config file
* you must also make sure that you have the correct IAM permissions in the biz-ops-projects project in Google Cloud
* that should work! basically, the bigquery client will automatically look for the credentials stored in the filepath in your .config

### Working / Ideas
* pd.read_csv is really weird on some of the files
* do the plans change quarter over quarter? I think not, but I am not sure
    * maybe the plan details change and thus the plans file changes?
* what if we want to run the script to upload new pricings when it comes out?
    * ex: in Q3, we want to quote the carriers available and don't want to wait until the information is fully loaded
    * can we think about running the script / creating logic to delete the pricings for the current quarter and then add the new ones in?
