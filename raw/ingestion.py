#%%
import datetime
import requests
import boto3
import json

#%%
class Ingestor:
    def __init__(self, url, per_page, bucket_name) -> None:
        self.url = url
        self.params = {
            "per_page": per_page, 
            "strategy": "new",
            "page": 1
        }

        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')

    def get_response(self, **params):
        return requests.get(self.url, params=params)
    
    def get_data(self, **params):
        return self.get_response(**params).json()
    
    def save_data(self, data):

        new_data = bytes(json.dumps(data).encode("UTF-8"))
        sufix = self.url.strip("/").split("/")[-1]
        date_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
        
        (self.s3_client
            .put_object(Body=new_data, 
                        Bucket= self.bucket_name, 
                        Key=f'tabnews/{sufix}/{date_str}.json'))
        
    def get_until_dates(self, date_start, date_stop):
        datetime_start = datetime.datetime.strptime(date_start, '%Y-%m-%d').date()
        datetime_stop = datetime.datetime.strptime(date_stop, '%Y-%m-%d').date()
        
        while datetime_start >= datetime_stop:
            print(self.params['page'])
            data = self.get_data(**self.params)
            self.save_data(data)
            self.params['page'] += 1
            datetime_start = datetime.datetime.fromisoformat(data[-1]['created_at']).date()


    
ingestorzin = Ingestor(
    url = 'https://www.tabnews.com.br/api/v1/contents', 
    per_page = 100,
    bucket_name= 'platform-datalake-yan')

ingestorzin.get_until_dates('2023-06-15', '2023-06-01')



# %%
