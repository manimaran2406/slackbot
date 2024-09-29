import os
from dotenv import load_dotenv
from io import StringIO
import boto3
import sys
from slack_bolt import App

# Function to load the .env file from S3 and apply environment variables
def load_env_from_s3(bucket_name, env_file_key):
    s3 = boto3.client('s3')
    try:
        # Fetch the .env file from the S3 bucket
        s3_object = s3.get_object(Bucket=bucket_name, Key=env_file_key)
        env_content = s3_object['Body'].read().decode('utf-8')

        # Load the .env content into environment variables
        load_dotenv(stream=StringIO(env_content))

        # Print for debugging purposes
        print("Successfully loaded .env from S3")

    except s3.exceptions.NoSuchKey:
        print(f".env file not found in S3 bucket {bucket_name}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading .env file from S3: {e}")
        sys.exit(1)

# Define your bucket and .env file key
BUCKET_NAME = 'slackflake-credentials'
ENV_FILE_KEY = '.env'

# Call the function to load environment variables from S3
load_env_from_s3(BUCKET_NAME, ENV_FILE_KEY)

# Check if the Slack bot token was loaded correctly
slack_bot_token = os.getenv("SLACK_BOT_TOKEN")
if not slack_bot_token:
    print("Error: SLACK_BOT_TOKEN is not set in the environment.")
    sys.exit(1)

# Initialize the Slack app
app = App(token=slack_bot_token)
