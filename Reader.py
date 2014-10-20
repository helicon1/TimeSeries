# -*- coding: utf-8 -*-
"""
Created on Mon Oct 20 20:08:12 2014

@author: des
"""

import json
from pymongo import MongoClient 

class ReadCustomerData(customerId):
    clName = "Readings"
    dbName = "Readings"    
    
    def __init__(self):
        self.client = MongoClient('localhost', 27017)    
        self.db = self.client[dbName]
        self.col = db[clName]
        self.data = col.find_one({'customerId': customerId })
        
    
    def GetYearsConsumption():
        
        
        