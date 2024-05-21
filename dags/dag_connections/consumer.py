from kafka import KafkaConsumer
import requests
import re
import pandas as pd
from json import loads
import json
from time import sleep

from api_endp import endpoint

print("Kafka Consumer App Start!")
 
consumer = KafkaConsumer('jobs_data',
                        # auto_offset_reset='earliest', 
                        enable_auto_commit=True, 
                        group_id='my-group-1',
                        value_deserializer=lambda m: loads(m.decode('utf-8')),
                        bootstrap_servers=['localhost:9092'])

batch = [] 

for data_json in consumer:
    s = data_json.value
    print(s)
    # receiving batch as a str with line breaks (\n)
    rows = s.split('\n') # splitting each row

    # processig each row in batch
    for row in rows:
    #    try loading row as a dict
        try:
            row_dict = loads(row)
            if isinstance(row_dict, dict):
                # sneding each row individually
                headers = {'Content-Type': 'application/json'}
                response = requests.post(endpoint, json=row_dict, headers=headers)

                if response.status_code == 200:
                    print("Datos enviados exitosamente a Power BI")
                else:
                    print(f"Fallo al enviar datos a Power BI. Código de estado: {response.status_code}")


        except json.JSONDecodeError as e:
            print(f"Error al decodificar JSON: {e}")
    
        sleep(2)