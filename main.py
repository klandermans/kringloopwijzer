import pandas as pd
import requests
import os
from bs4 import BeautifulSoup
import time
import hashlib 
import sys   
import os.path

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.storage.common.models import LocationMode
from azure.storage.common.retry import (
    ExponentialRetry,
    LinearRetry,
    no_retry,)    


class kringloopwijzer:
    
    data = {'signaleringen':[],'duurzaamheid':[], 'dumpfiles':[], 'kengetallen':[]}

    
    def parse(self, key, filename):
        soup = BeautifulSoup(open(filename, encoding="utf8"), "lxml")
        result = soup.find(key)
        
        farm = filename.replace('../data/kringloopwijzer/','').split(' - ')[0]
        
        for child in result.children:
            if child.name != None:
                row = {'filename':farm,'type':child.name}
                for c in child.children:
                    if c.name != None:
                        for d in c.children:
                            row[c.name] = str(d)
                self.data[key].append(row)

    def __init__(self):  
        krw = ['duurzaamheid', 'dumpfiles', 'kengetallen','signaleringen']

        files = os.scandir('../data/kringloopwijzer')
     
        for file in files:
            file = file.name
             
            if 'xml' in file:
                for key in krw:    
                    self.parse(key=key, filename="../data/"+file)

                for key in krw:    
                    df = pd.DataFrame().from_dict(self.data[key])
                    df.to_excel(key+'.xlsx')
                    df.to_parquet(key+'.parquet')
                    
            


kringloopwijzer()
