import logging
from io import StringIO
import boto3

class S3Logger(logging.Handler):
    def __init__(self, bucket_name, log_file_name):
        super().__init__()
        self.bucket_name = bucket_name
        self.log_file_name = log_file_name
        self.log_stream = StringIO()
        self.s3_client = boto3.client('s3')

    def emit(self, record):
        log_entry = self.format(record)
        self.log_stream.write(log_entry + '\n')
        self.upload_to_s3()

    def upload_to_s3(self):
        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=self.log_file_name, Body=self.log_stream.getvalue())
        except Exception as e:
            print(f"Error uploading log to S3: {str(e)}")

def configure_logger(bucket_name, log_file_name):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    s3_handler = S3Logger(bucket_name, log_file_name)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    s3_handler.setFormatter(formatter)
    
    logger.addHandler(s3_handler)
    return logger
