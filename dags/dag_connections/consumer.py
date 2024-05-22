from kafka import KafkaConsumer
import requests
import re
import pandas as pd
from json import loads
import json
from time import sleep


print("Kafka Consumer App Start!")
 
consumer = KafkaConsumer('linkedin.streaming',
                        auto_offset_reset='earliest', 
                        enable_auto_commit=True, 
                        group_id='my-group-1',
                        value_deserializer=lambda m: loads(m.decode('utf-8')),
                        bootstrap_servers=['localhost:9092'])

batch = [] 

for data_json in consumer:
    url = "https://api.powerbi.com/beta/693cbea0-4ef9-4254-8977-76e05cb5f556/datasets/e19871ee-8b00-453e-9cc2-346d578d6405/rows?experience=power-bi&key=MZnoxSrGeQcNOUT7sZXvIVlnWXxBd6WT9MeEmhD8wNcBVVayn%2BVCIuc1nrsJWmZGhFFuYpytWtmYJ6qqWAF6iw%3D%3D"
    s = data_json.value
    print(s)
   
    rows = s.split('\n') 

    
    for row in rows:
    
        try:
            row_dict = loads(row)
            if isinstance(row_dict, dict):
                
                headers = {'Content-Type': 'application/json'}
                response = requests.post(url, json=row_dict, headers=headers)

                if response.status_code == 200:
                    print("Data successfully sent to Power BI")
                else:
                    print(f"Failed to send data to Power BI. Status code: {response.status_code}")


        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
    
        sleep(2)

