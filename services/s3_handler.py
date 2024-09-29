import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime

class S3Handler:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3')

    def save_to_s3(self, data, columns):
        df = pd.DataFrame(data, columns=columns)
        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Results')
            workbook = writer.book
            worksheet = writer.sheets['Results']
            for i, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, max_len)
        excel_buffer.seek(0)

        file_key = f'{datetime.now().strftime("%d-%m-%Y")}/results_{datetime.now().strftime("%d-%m-%H-%M-%S")}.xlsx'

        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=file_key, Body=excel_buffer.getvalue())
            return self.s3.generate_presigned_url('get_object', Params={'Bucket': self.bucket_name, 'Key': file_key}, ExpiresIn=1800)
        except Exception as e:
            print(f"Error uploading file to S3: {str(e)}")
            return None
