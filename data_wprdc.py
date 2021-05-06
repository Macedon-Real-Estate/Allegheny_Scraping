import pandas as pd
import boto3
import io
import requests
import s3fs

access_id = ''
access_key = '' 
s3 = boto3.client('s3', aws_access_key_id = access_id, aws_secret_access_key = access_key)





url="https://tools.wprdc.org/downstream/8eff881d-4d28-4064-83f1-30cc991cfec7"
s=requests.get(url).content
c=pd.read_csv(io.StringIO(s.decode('utf-8')))
pittsburgh = c[c.MUNICIPALITY=='Pittsburgh']

csv_buffer =io.StringIO()
pittsburgh.to_csv(csv_buffer)
s3.put_object(Bucket='rasit-documents', Key='data_wprdc_property_assesment.csv', Body=csv_buffer.getvalue())


