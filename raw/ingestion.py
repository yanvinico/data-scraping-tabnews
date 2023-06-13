#%%
import requests
import boto3
import json

#%%
class Ingestor:
    def __init__(self, url, per_page, strategy, stop_date, bucket_name) -> None:
        self.url = url
        self.params = {
            'per_page': per_page, 
            'strategy': strategy,
        }

        self.stop_date = stop_date
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')

    def get_response(self, **params):
        return requests.get(self.url, params=params)
    
    def get_data(self, **params):
        return self.get_response(**params).json()
    
    def save_data(self, data):

        new_data = bytes(json.dumps(data).encode("UTF-8"))
        
        sufix = self.url.strip("/").split("/")[-1]
        
        (self.s3_client
            .put_objetc(Body=new_data, 
                        Bucket= self.bucket_name, 
                        Key=f'tabnews/{sufix}'))
            

    
ingestorzin = Ingestor(
    url = 'https://www.tabnews.com.br/api/v1/contents', 
    per_page = 100,
    strategy = 'new',
    stop_date = '2023-06-13',
    bucket_name= 'platform-datalake-yan')

data = ingestorzin.get_data(**ingestorzin.params)


# %%
