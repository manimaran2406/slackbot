import os
import boto3
from dotenv import load_dotenv
from io import StringIO

def load_env_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    try:
        s3_object = s3.get_object(Bucket=bucket_name, Key=file_key)
        env_content = s3_object['Body'].read().decode('utf-8')
        env_stream = StringIO(env_content)
        load_dotenv(stream=env_stream)
    except boto3.exceptions.Boto3Error as e:
        print(f"Error fetching the .env file from S3: {e}")

def get_config():
    load_env_from_s3('slackflake-credentials', '.env')
    return {
        'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
        'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
        'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
        'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
        'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'),
        'SLACK_APP_TOKEN': os.getenv('SLACK_APP_TOKEN'),
        'SLACK_BOT_TOKEN': os.getenv('SLACK_BOT_TOKEN'),
        'LDAP_GROUP': os.getenv('LDAP_GROUP'),
        'API_KEY': os.getenv('API_KEY'),
        'AWS_BUCKET_NAME': 'slackflake-storage',
        'AWS_BUCKET_NAME_1': 'slackflake-log'
    }
