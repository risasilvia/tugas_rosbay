import numpy as np
import pandas as pd
import math
import time
from pymongo import MongoClient
from pprint import pprint
from parserpackage import AddressParser

# Connection to database
client = MongoClient(port=27017)
db = client.ptposAddress # mongodb database in server hadoop@demo.rosebaycorporate.com -p 3002

start = 6878
batch_size = 100000    # Batch size
counter = 1  # starting from 1 because we are ignoring the first column which is the header name
# Counter is used to skip the batches
i = 6878

while counter <= 1:   # set the batch
    # start = counter * batch_size
    df = pd.read_csv('alamat penerima januari reg 4 5.csv', header=None, names=['Addresses'], skiprows=start, nrows=batch_size)
    start = (counter * batch_size)+1
    # update to start and end rows
    counter+=1
    
    for address in df['Addresses']:
        data = {}
        print('Data processing : {}'.format(i))
        data['raw address'] = address
        print('Raw address : {}'.format(address))

        test_object = AddressParser()
        data['parse address'] = test_object.parse_address(address)
        result = db.addresses.insert_one(data)  # Save data in database
        print('Data processed : {}'.format(i))

        i+=1
        time.sleep(0.5)
        
    